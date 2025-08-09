package treiding.hpq.userservice.service;

import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import treiding.hpq.userservice.entity.User;
import treiding.hpq.userservice.exception.specific.UserNotFoundException;
import treiding.hpq.userservice.repository.UserRepository;
import treiding.hpq.userservice.security.CustomUserDetails;

@Service
public class CustomUserDetailsService implements UserDetailsService {
    private final UserRepository userRepository;

    public CustomUserDetailsService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public UserDetails loadUserByUsername(String email) throws UsernameNotFoundException {
        User user = userRepository.findByEmail(email)
                .orElseThrow(()-> new UserNotFoundException("Email: "+email + " not found"));
        return new CustomUserDetails(user);
    }
}
