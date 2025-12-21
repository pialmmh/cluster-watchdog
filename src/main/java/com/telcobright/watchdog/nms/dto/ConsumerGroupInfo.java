package com.telcobright.watchdog.nms.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Consumer group information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupInfo {
    private String groupId;
    private String state;
    private int memberCount;
    private List<String> topics;
    private int coordinatorId;
    private String coordinatorHost;
}
