package treiding.hpq.userservice.controller;

import org.apache.coyote.Response;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import treiding.hpq.userservice.entity.User;
import treiding.hpq.userservice.service.UserService;

import java.util.List;

@RestController
@RequestMapping("api/users")
public class UserAPIController {

    private final UserService userService;

    public UserAPIController(UserService userService) {
        this.userService = userService;
    }

    @PreAuthorize("hasAnyRole('ADMIN', 'TRADER')")
    @GetMapping
    public ResponseEntity<Page<User>> getAllUsers(Pageable pageable) {
        return new ResponseEntity<>(userService.getAllUser(pageable), HttpStatus.OK);
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PutMapping("{id}")
    public ResponseEntity<User> updateUser(@PathVariable("id") String userId, @RequestBody User userDetails) {
        return new ResponseEntity<>(userService.updateUser(userId, userDetails), HttpStatus.OK);
    }

    @PreAuthorize("hasRole('ADMIN')")
    @DeleteMapping("{id}")
    public String deleteUser(@PathVariable("id") String userId) {
        userService.deleteUser(userId);
        return "User deleted successfully";
    }
}
