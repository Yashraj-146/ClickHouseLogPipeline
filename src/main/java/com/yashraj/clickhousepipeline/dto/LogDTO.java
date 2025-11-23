package com.yashraj.clickhousepipeline.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogDTO {

    @NotNull(message = "timestamp is required")
    private LocalDateTime timestamp;

    @NotBlank(message = "level is required")
    private String level;

    @NotBlank(message = "message is required")
    private String message;

    private String service = "unknown";
}
