package com.artillexstudios.axvaults.database.messaging;

import java.util.UUID;

public interface Messenger {

    void broadcast(ChangeType type, int vaultId, UUID playerUuid);

    default void start() {}

    default void stop() {}

    enum ChangeType {
        DELETE, UPDATE, INSERT;

        public static final ChangeType[] entries = ChangeType.values();
    }
}
