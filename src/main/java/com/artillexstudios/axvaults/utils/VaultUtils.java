package com.artillexstudios.axvaults.utils;

import com.artillexstudios.axapi.serializers.Serializers;
import com.artillexstudios.axapi.utils.logging.LogUtils;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.vaults.Vault;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public class VaultUtils {
    private static boolean asyncItemSerializer;
    private static boolean deleteEmptyVaults;

    public static void reload() {
        asyncItemSerializer = CONFIG.getBoolean("async-item-serializer", false);
        deleteEmptyVaults = CONFIG.getBoolean("delete-empty-vaults", true);
    }

    public static CompletableFuture<Void> save(Vault vault) {
        CompletableFuture<Object> local = new CompletableFuture<>();
        ThreadUtils.runAsync(() -> VaultUtils.serialize(vault, local));

        CompletableFuture<Void> cf = new CompletableFuture<>();
        local.exceptionally(throwable -> {
            LogUtils.error("An exception occurred while saving vaults!", throwable);
            return null;
        }).thenAccept(result -> {
            AxVaults.getDatabase().saveVault(vault, result, false);
            cf.complete(null);
        });
        return cf;
    }

    public static void saveSync(@NotNull Vault vault) {
        AxVaults.getDatabase().saveVault(vault, getSerialized(vault), true);
    }

    public static Object getSerialized(@NotNull Vault vault) {
        if (deleteEmptyVaults && vault.getStorage().isEmpty()) {
            return true;
        } else {
            try {
                return Serializers.ITEM_ARRAY.serialize(vault.getStorage().getContents());
            } catch (Exception ex) {
                ex.printStackTrace();
                return null;
            }
        }
    }

    public static void serialize(Vault vault, CompletableFuture<Object> future) {
        Runnable runnable = () -> {
            if (deleteEmptyVaults && vault.getStorage().isEmpty()) {
                future.complete(true); // delete
                return;
            }

            try {
                future.complete(Serializers.ITEM_ARRAY.serialize(vault.getStorage().getContents())); // success
            } catch (Exception ex) {
                ex.printStackTrace();
                future.complete(null); // error
            }
        };

        if (asyncItemSerializer) runnable.run();
        else ThreadUtils.runSync(runnable);
    }

    public static boolean isAsyncItemSerializer() {
        return asyncItemSerializer;
    }

    public static boolean isDeleteEmptyVaults() {
        return deleteEmptyVaults;
    }
}
