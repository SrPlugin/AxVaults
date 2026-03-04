package com.artillexstudios.axvaults.database.messaging;

import com.artillexstudios.axapi.executor.ExceptionReportingScheduledThreadPool;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.impl.MySQL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public class SQLMessaging implements Messenger {
    private final MySQL db;
    private ScheduledExecutorService executor = null;

    public SQLMessaging(MySQL db) {
        this.db = db;
    }

    @Override
    public void start() {
        if (executor != null) executor.shutdown();
        executor = new ExceptionReportingScheduledThreadPool(1);
        executor.scheduleAtFixedRate(db::checkForChanges, 1, 1, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(db::removeOldChanges, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        if (executor == null) return;
        executor.shutdownNow();
    }

    @Override
    public void broadcast(ChangeType type, int vaultId, UUID playerUuid, boolean sync) {
        final String sql = "INSERT INTO axvaults_messages(event, vault_id, uuid, date) VALUES (?, ?, ?, ?);";
        Runnable runnable = () -> {
            try (Connection conn = db.getDataSource().getConnection(); PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                stmt.setShort(1, (short) type.ordinal());
                stmt.setInt(2, vaultId);
                stmt.setString(3, playerUuid.toString());
                stmt.setLong(4, System.currentTimeMillis());
                stmt.executeUpdate();
                try (ResultSet rs = stmt.getGeneratedKeys()) {
                    if (rs.next()) db.getSentFromHere().put(rs.getInt(1), System.currentTimeMillis() + 10_000L);
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        };

        if (sync) runnable.run();
        else AxVaults.getThreadedQueue().submit(runnable);
    }
}
