package treiding.hpq.userservice.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import treiding.hpq.userservice.entity.KycStatus;
import treiding.hpq.userservice.entity.Role;
import treiding.hpq.userservice.entity.User;
import treiding.hpq.userservice.entity.UserProfile;
import treiding.hpq.userservice.exception.specific.EmailAlreadyExistsException;
import treiding.hpq.userservice.exception.specific.UserNotFoundException;
import treiding.hpq.userservice.repository.UserRepository;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;


    public UserService(UserRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    public User createUser(User newUser) {
        // check if emails already exists
        if (userRepository.findByEmail(newUser.getEmail()).isPresent()) {
            throw new EmailAlreadyExistsException(
                    "Email: " + newUser.getEmail() + " already exists"
            );
        }

        // Hash password
        String hashedPassword = passwordEncoder.encode(newUser.getPassword());
        newUser.setPassword(hashedPassword);

        // handle User
        newUser.setUserId(UUID.randomUUID().toString());
        newUser.setKycStatus(KycStatus.PENDING);
        newUser.setRole(Role.TRADER);
        newUser.setActive(true);
        newUser.setCreatedAt(LocalDateTime.now());

        // handle UserProfile
        UserProfile requestProfile = newUser.getUserProfile();
        if (requestProfile != null) {
            requestProfile.setProfileId(UUID.randomUUID().toString());
            requestProfile.setUser(newUser);
            newUser.setUserProfile(requestProfile);
        }

        // save
        return userRepository.save(newUser);
    }

    public User getUserById(String id) {
        User user = userRepository.findByUserId(id)
                .orElseThrow(() -> new UserNotFoundException("User not found with id: " + id));
        return user;
    }

    public Page<User> getAllUser(Pageable pageable) {
        return userRepository.findAll(pageable);
    }

    public User updateUser(String userId, User userDetails) {
        User existingUser = userRepository.findByUserId(userId)
                .orElseThrow(()-> new UserNotFoundException("User with id: " + userId+" not found"));

        // check if email already exist
        if (!existingUser.getEmail().equals(userDetails.getEmail()) &&
                userRepository.findByEmail(userDetails.getEmail()).isPresent()) {
            throw new EmailAlreadyExistsException("Email '" + userDetails.getEmail() + "' already in use.");
        }

        existingUser.setEmail(userDetails.getEmail());
        existingUser.setActive(userDetails.isActive());

        return userRepository.save(existingUser);
    }

    public void deleteUser(String userId) {
        if (userRepository.findByUserId(userId).isEmpty()) {
            throw new UserNotFoundException("User with ID: " + userId + " not found");
        }
        userRepository.deleteByUserId(userId);
    }
}
