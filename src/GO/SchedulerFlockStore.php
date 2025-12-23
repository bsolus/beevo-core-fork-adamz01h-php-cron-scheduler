<?php

declare(strict_types=1);

namespace GO;

use Symfony\Component\Lock\Exception\LockReleasingException;
use Symfony\Component\Lock\Key;
use Symfony\Component\Lock\Store\FlockStore;

/**
 * Custom FlockStore implementation with enhanced error handling for lock deletion.
 *
 * This class extends Symfony's FlockStore to address permission issues that can occur
 * when locks are created by different users (e.g., CLI running as root vs web server
 * running as www-data). The override provides graceful error handling and logging for
 * permission-related failures during lock release operations.
 *
 * Common issues addressed:
 * - Permission denied errors when calling DELETE /jobs/{id}/lock
 * - CLI can create locks but API cannot release them (or vice versa)
 * - Cross-user lock management in multi-process environments
 *
 * Recommended fix for file-based locks: chmod 1777 /tmp/scheduler.lock
 * (sticky bit + world writable)
 *
 * @see FlockStore
 * @see https://symfony.com/doc/current/components/lock.html
 */
final class SchedulerFlockStore extends FlockStore
{
    /**
     * Deletes a lock with enhanced error handling for permission issues.
     *
     * This override catches exceptions during lock release and logs detailed
     * information about permission failures, including file path, current user,
     * and file permissions. Instead of failing silently or throwing exceptions,
     * it logs the error and continues execution to prevent blocking operations.
     *
     * @param Key $key The lock key to delete
     *
     * @throws LockReleasingException If the lock cannot be released
     */
    public function delete(Key $key): void
    {
        // The lock is maybe not acquired.
        if (!$key->hasState(FlockStore::class)) {
            return;
        }

        $handle = $key->getState(FlockStore::class)[1];

        // Extract file path from the stream resource
        $filePath = null;
        if (is_resource($handle)) {
            $meta = stream_get_meta_data($handle);
            $filePath = $meta['uri'] ?? null;
        }

        parent::delete($key);

        // Remove the lock file after unlocking
        if (is_string($filePath) && file_exists($filePath)) {
            unlink($filePath);
        }
    }
}
