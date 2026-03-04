package com.artillexstudios.axvaults.database.messaging;

import com.artillexstudios.axapi.executor.ThreadedQueue;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.impl.MySQL;
import com.artillexstudios.axvaults.vaults.Vault;
import com.artillexstudios.axvaults.vaults.VaultManager;
import com.artillexstudios.axvaults.vaults.VaultPlayer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.UUID;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public class RedisMessenger implements Messenger {
    private final MySQL db;
    private JedisPool pool;
    private JedisPubSub pubSub;
    private final String channel;
    private final ThreadedQueue<Runnable> queue = new ThreadedQueue<>("AxVaults-Redis-thread");

    public RedisMessenger(MySQL db) {
        this.db = db;
        this.channel = CONFIG.getString("messaging.redis.channel", "axvaults:messaging");
    }

    @Override
    public void start() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(16);
        
        String address = CONFIG.getString("messaging.redis.address", "localhost");
        int port = CONFIG.getInt("messaging.redis.port", 6379);
        String password = CONFIG.getString("messaging.redis.password", "");
        
        if (password.isEmpty()) {
            pool = new JedisPool(config, address, port);
        } else {
            pool = new JedisPool(config, address, port, 2000, password);
        }

        pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                // event;vault_id;uuid
                String[] split = message.split(";");
                if (split.length != 3) return;

                Messenger.ChangeType type = Messenger.ChangeType.valueOf(split[0]);
                int vaultId = Integer.parseInt(split[1]);
                UUID uuid = UUID.fromString(split[2]);

                if (type == ChangeType.UPDATE) {
                    VaultPlayer vp = VaultManager.getPlayers().get(uuid);
                    if (vp == null) return;
                    Vault vault = vp.getVault(vaultId);
                    if (vault == null) return;
                    
                    db.updateVault(vault);
                }
            }
        };

        queue.submit(() -> {
            try (Jedis jedis = pool.getResource()) {
                jedis.subscribe(pubSub, channel);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    @Override
    public void stop() {
        if (pubSub != null) pubSub.unsubscribe();
        if (pool != null) pool.close();
        queue.stop();
    }

    @Override
    public void broadcast(ChangeType type, int vaultId, UUID playerUuid) {
        queue.submit(() -> {
            try (Jedis jedis = pool.getResource()) {
                jedis.publish(channel, type.name() + ";" + vaultId + ";" + playerUuid.toString());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }
}
