package treiding.hpq.userservice.entity;

import jakarta.persistence.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table (name = "kycs")
public class KYC {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Unique identifier for KYC
     */
    private String kycId;

    private String userId;

    private DocumentType documentType;

    private String documentNumber;

    private String frontDocumentUrl;

    private String backDocumentUrl;

    private String selfieUrl;

    private String submissionDate;

    private LocalDateTime reviewDate;

    private KycStatus status;
}
