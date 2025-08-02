package treiding.hpq.matchingservice.entity;

import redis.clients.jedis.*;
import treiding.hpq.basedomain.entity.Order;
import treiding.hpq.basedomain.entity.OrderSide;
import treiding.hpq.basedomain.entity.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class RedisOrderBook {

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

    public JedisPool getJedisPool() { return jedisPool; }
    public String getBidKey() { return BID_KEY; }
    public String getAskKey() { return ASK_KEY; }
    public static String getOrderDetailKey(String orderId) { return ORDER_DETAIL_PREFIX + orderId; }

    public void addOrder(Order order) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();

            Map<String, String> orderData = orderToHashMap(order);
            t.hset(ORDER_DETAIL_PREFIX + order.getOrderId(), orderData);

            String targetKey = (order.getSide() == OrderSide.BUY) ? BID_KEY : ASK_KEY;
            double score = order.getPrice().doubleValue();
            t.zadd(targetKey, score, order.getOrderId());

            t.exec();
            System.out.println("DEBUG: Đã thêm/cập nhật lệnh " + order.getOrderId() + " vào Redis.");
        } catch (Exception e) {
            System.err.println("Lỗi khi thêm/cập nhật lệnh " + order.getOrderId() + " vào Redis: " + e.getMessage());
            throw new RuntimeException("Lỗi Redis khi thêm lệnh", e);
        }
    }

    public void removeOrder(Order order) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();

            String targetKey = (order.getSide() == OrderSide.BUY) ? BID_KEY : ASK_KEY;
            t.zrem(targetKey, order.getOrderId());
            t.del(ORDER_DETAIL_PREFIX + order.getOrderId());

            List<Object> results = t.exec();
            Long zremResult = (Long) results.get(0);

            if (zremResult > 0) {
                System.out.println("DEBUG: Đã xóa lệnh " + order.getOrderId() + " khỏi Redis.");
            } else {
                System.out.println("DEBUG: Lệnh " + order.getOrderId() + " không tìm thấy trong Redis Sorted Set để xóa.");
            }
        } catch (Exception e) {
            System.err.println("Lỗi khi xóa lệnh " + order.getOrderId() + " khỏi Redis: " + e.getMessage());
            throw new RuntimeException("Lỗi Redis khi xóa lệnh", e);
        }
    }

    public Iterator<Map.Entry<BigDecimal, List<Order>>> getBidLevelsIterator() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> orderIds = new HashSet<>(jedis.zrevrange(BID_KEY, 0, -1));  // fix kiểu dữ liệu
            return getOrderLevelsIterator(orderIds, true);
        }
    }

    public Iterator<Map.Entry<BigDecimal, List<Order>>> getAskLevelsIterator() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> orderIds = new HashSet<>(jedis.zrange(ASK_KEY, 0, -1));  // fix kiểu dữ liệu
            return getOrderLevelsIterator(orderIds, false);
        }
    }

    private Iterator<Map.Entry<BigDecimal, List<Order>>> getOrderLevelsIterator(Set<String> orderIds, boolean isBid) {
        if (orderIds.isEmpty()) return Collections.emptyIterator();

        List<Order> orders = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            List<Response<Map<String, String>>> responses = new ArrayList<>();

            for (String orderId : orderIds) {
                responses.add(p.hgetAll(ORDER_DETAIL_PREFIX + orderId));
            }
            p.sync();

            int i = 0;
            for (String orderId : orderIds) {
                Map<String, String> orderData = responses.get(i++).get();
                if (orderData != null && !orderData.isEmpty()) {
                    orders.add(hashMapToOrder(orderData));
                } else {
                    System.err.println("Cảnh báo: Không tìm thấy lệnh " + orderId + " trong Redis.");
                }
            }
        }

        Map<BigDecimal, List<Order>> groupedOrders = orders.stream()
                .collect(Collectors.groupingBy(Order::getPrice, LinkedHashMap::new, Collectors.toList()));

        List<BigDecimal> sortedPrices = orders.stream()
                .map(Order::getPrice)
                .distinct()
                .sorted(isBid ? Comparator.reverseOrder() : Comparator.naturalOrder())
                .toList();

        LinkedHashMap<BigDecimal, List<Order>> finalGroupedOrders = new LinkedHashMap<>();
        for (BigDecimal price : sortedPrices) {
            finalGroupedOrders.put(price, groupedOrders.get(price));
        }

        return finalGroupedOrders.entrySet().iterator();
    }

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
                        .map(id -> ORDER_DETAIL_PREFIX + id)
                        .toArray(String[]::new);
                t.del(orderDetailKeys);
            }

            t.exec();
            System.out.println("Order book cho " + instrumentId + " đã được xóa.");
        } catch (Exception e) {
            System.err.println("Lỗi khi xóa Order Book cho " + instrumentId + ": " + e.getMessage());
            throw new RuntimeException("Lỗi Redis khi xóa sổ lệnh", e);
        }
    }

    private Map<String, String> orderToHashMap(Order order) {
        Map<String, String> orderData = new HashMap<>();
        orderData.put("orderId", order.getOrderId());
        orderData.put("instrumentId", order.getInstrumentId());
        orderData.put("side", order.getSide().name());
        orderData.put("price", order.getPrice().toPlainString());
        orderData.put("originalQuantity", order.getOriginalQuantity().toPlainString());
        orderData.put("remainingQuantity", order.getRemainingQuantity().toPlainString());
        orderData.put("status", order.getStatus().name());
        orderData.put("timestamp", String.valueOf(order.getTimestamp().toEpochMilli()));
        return orderData;
    }

    public Order hashMapToOrder(Map<String, String> orderData) {
        if (orderData == null || orderData.isEmpty()) return null;
        return new Order(
                orderData.get("orderId"),
                orderData.get("instrumentId"),
                OrderSide.valueOf(orderData.get("side")),
                new BigDecimal(orderData.get("price")),
                new BigDecimal(orderData.get("originalQuantity")),
                new BigDecimal(orderData.get("remainingQuantity")),
                OrderStatus.valueOf(orderData.get("status")),
                Instant.ofEpochMilli(Long.parseLong(orderData.get("timestamp")))
        );
    }

    public Order getOrderDetail(String orderId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> orderData = jedis.hgetAll(ORDER_DETAIL_PREFIX + orderId);
            return hashMapToOrder(orderData);
        }
    }
}
