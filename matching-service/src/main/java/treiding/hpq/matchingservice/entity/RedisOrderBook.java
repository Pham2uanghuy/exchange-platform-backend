package treiding.hpq.matchingservice.entity;

import redis.clients.jedis.*;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderSide;
import treiding.hpq.basedomain.entity.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RedisOrderBook implements OrderBook {

    private final JedisPool jedisPool;
    private final String instrumentId;

    private final String BID_KEY;
    private final String ASK_KEY;
    private static final String ORDER_DETAIL_PREFIX = "order:";

    public RedisOrderBook(JedisPool jedisPool, String instrumentId) {
        this.jedisPool = jedisPool;
        this.instrumentId = instrumentId;
        this.BID_KEY = "orderbook:" + instrumentId + ":bids";
        this.ASK_KEY = "orderbook:" + instrumentId + ":asks";
    }

    @Override
    public void addOrder(Order order) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();

            Map<String, String> orderData = orderToHashMap(order);
            t.hset(getOrderDetailKey(order.getOrderId()), orderData);

            String targetKey = (order.getSide() == OrderSide.BUY) ? BID_KEY : ASK_KEY;
            double score = order.getPrice().doubleValue();
            t.zadd(targetKey, score, order.getOrderId());

            t.exec();
        } catch (Exception e) {
            throw new RuntimeException("Lỗi Redis khi thêm lệnh " + order.getOrderId(), e);
        }
    }

    @Override
    public void updateOrder(Order updatedOrder) {
        try (Jedis jedis = jedisPool.getResource()) {
            Order existingOrder = getOrderDetail(updatedOrder.getOrderId());
            if (existingOrder == null) {
                // Nếu lệnh không tồn tại, có thể xem xét thêm mới hoặc ném ngoại lệ
                addOrder(updatedOrder);
                return;
            }

            Transaction t = jedis.multi();

            // Xóa lệnh cũ khỏi Sorted Set nếu giá hoặc bên mua/bán thay đổi
            if (!existingOrder.getPrice().equals(updatedOrder.getPrice()) || existingOrder.getSide() != updatedOrder.getSide()) {
                String oldTargetKey = (existingOrder.getSide() == OrderSide.BUY) ? BID_KEY : ASK_KEY;
                t.zrem(oldTargetKey, updatedOrder.getOrderId());

                String newTargetKey = (updatedOrder.getSide() == OrderSide.BUY) ? BID_KEY : ASK_KEY;
                double newScore = updatedOrder.getPrice().doubleValue();
                t.zadd(newTargetKey, newScore, updatedOrder.getOrderId());
            }

            // Cập nhật chi tiết lệnh trong Hash
            Map<String, String> orderData = orderToHashMap(updatedOrder);
            t.hset(getOrderDetailKey(updatedOrder.getOrderId()), orderData);

            t.exec();
        } catch (Exception e) {
            throw new RuntimeException("Lỗi Redis khi cập nhật lệnh " + updatedOrder.getOrderId(), e);
        }
    }

    @Override
    public void removeOrder(Order order) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();

            String targetKey = (order.getSide() == OrderSide.BUY) ? BID_KEY : ASK_KEY;
            t.zrem(targetKey, order.getOrderId());
            t.del(getOrderDetailKey(order.getOrderId()));

            t.exec();
        } catch (Exception e) {
            throw new RuntimeException("Lỗi Redis khi xóa lệnh " + order.getOrderId(), e);
        }
    }

    @Override
    public Iterator<Map.Entry<BigDecimal, List<Order>>> getBidLevelsIterator(String instrumentId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> orderIds = new HashSet<>(jedis.zrevrange(BID_KEY, 0, -1));
            return getOrderLevelsIterator(orderIds, true);
        }
    }

    @Override
    public Iterator<Map.Entry<BigDecimal, List<Order>>> getAskLevelsIterator(String instrumentId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> orderIds = new HashSet<>(jedis.zrange(ASK_KEY, 0, -1));
            return getOrderLevelsIterator(orderIds, false);
        }
    }

    private Iterator<Map.Entry<BigDecimal, List<Order>>> getOrderLevelsIterator(Set<String> orderIds, boolean isBid) {
        if (orderIds.isEmpty()) {
            return Collections.emptyIterator();
        }

        List<Order> orders = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            List<Response<Map<String, String>>> responses = new ArrayList<>();

            for (String orderId : orderIds) {
                responses.add(p.hgetAll(getOrderDetailKey(orderId)));
            }
            p.sync();

            for (Response<Map<String, String>> response : responses) {
                Map<String, String> orderData = response.get();
                if (orderData != null && !orderData.isEmpty()) {
                    orders.add(hashMapToOrder(orderData));
                }
            }
        }

        Map<BigDecimal, List<Order>> groupedOrders = orders.stream()
                .collect(Collectors.groupingBy(Order::getPrice, LinkedHashMap::new, Collectors.toList()));

        List<BigDecimal> sortedPrices = groupedOrders.keySet().stream()
                .sorted(isBid ? Comparator.reverseOrder() : Comparator.naturalOrder())
                .collect(Collectors.toList());

        LinkedHashMap<BigDecimal, List<Order>> finalGroupedOrders = new LinkedHashMap<>();
        for (BigDecimal price : sortedPrices) {
            finalGroupedOrders.put(price, groupedOrders.get(price));
        }

        return finalGroupedOrders.entrySet().iterator();
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();

            Set<String> allOrderIds = new HashSet<>();
            allOrderIds.addAll(jedis.zrange(BID_KEY, 0, -1));
            allOrderIds.addAll(jedis.zrange(ASK_KEY, 0, -1));

            t.del(BID_KEY);
            t.del(ASK_KEY);

            if (!allOrderIds.isEmpty()) {
                String[] orderDetailKeys = allOrderIds.stream()
                        .map(RedisOrderBook::getOrderDetailKey)
                        .toArray(String[]::new);
                t.del(orderDetailKeys);
            }
            t.exec();
        } catch (Exception e) {
            throw new RuntimeException("Lỗi Redis khi xóa sổ lệnh cho " + instrumentId, e);
        }
    }

    @Override
    public Order getOrderDetail(String orderId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> orderData = jedis.hgetAll(getOrderDetailKey(orderId));
            return hashMapToOrder(orderData);
        }
    }

    private static String getOrderDetailKey(String orderId) {
        return ORDER_DETAIL_PREFIX + orderId;
    }

    private Map<String, String> orderToHashMap(Order order) {
        Map<String, String> orderData = new HashMap<>();
        orderData.put("id", String.valueOf(order.getId()));
        orderData.put("orderId", order.getOrderId());
        orderData.put("instrumentId", order.getInstrumentId());
        orderData.put("side", order.getSide().name());
        orderData.put("price", order.getPrice().toPlainString());
        orderData.put("originalQuantity", order.getOriginalQuantity().toPlainString());
        orderData.put("remainingQuantity", order.getRemainingQuantity().toPlainString());
        orderData.put("status", order.getStatus().name());
        orderData.put("timestamp", String.valueOf(order.getTimestamp().toEpochMilli()));
        orderData.put("userId", order.getUserId());
        return orderData;
    }

    private Order hashMapToOrder(Map<String, String> orderData) {
        if (orderData == null || orderData.isEmpty()) {
            return null;
        }
        return new Order(
                Long.valueOf(orderData.get("id")),
                orderData.get("orderId"),
                orderData.get("instrumentId"),
                orderData.get("userId"),
                OrderSide.valueOf(orderData.get("side")),
                new BigDecimal(orderData.get("price")),
                new BigDecimal(orderData.get("originalQuantity")),
                new BigDecimal(orderData.get("remainingQuantity")),
                OrderStatus.valueOf(orderData.get("status")),
                Instant.ofEpochMilli(Long.parseLong(orderData.get("timestamp")))
        );
    }
}