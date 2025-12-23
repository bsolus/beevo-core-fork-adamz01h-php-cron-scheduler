<?php

declare(strict_types=1);

namespace GO;

use DateTime;
use Exception;
use InvalidArgumentException;
use Redis;
use RuntimeException;
use Symfony\Component\Lock\BlockingStoreInterface;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\LockInterface;
use Symfony\Component\Lock\PersistingStoreInterface;
use Symfony\Component\Lock\SharedLockStoreInterface;
use Symfony\Component\Lock\Store\FlockStore;
use Symfony\Component\Lock\Store\PdoStore;
use Symfony\Component\Lock\Store\RedisStore;
use Symfony\Component\Lock\Store\SemaphoreStore;

/**
 * Job Scheduler with centralized configuration and lock management.
 *
 * @author  Your Name
 * @license MIT
 */
class Scheduler
{
    /**
     * The queued jobs.
     *
     * @var array<int, Job>
     */
    private array $jobs = [];

    /**
     * Successfully executed jobs.
     *
     * @var array<int, Job>
     */
    private array $executedJobs = [];

    /**
     * Failed jobs.
     *
     * @var array<int, FailedJob>
     */
    private array $failedJobs = [];

    /**
     * The verbose output of the scheduled jobs.
     *
     * @var array<int, string>
     */
    private array $outputSchedule = [];

    /**
     * Scheduler configuration.
     *
     * @var array{
     *   tempDir?: string,
     *   email?: array{
     *     subject?: string,
     *     from?: string,
     *     body?: string,
     *     transport?: mixed,
     *     ignore_empty_output?: bool
     *   },
     *   lock?: array{
     *     adapter?: string,
     *     prefix?: string,
     *     ttl?: int,
     *     auto_release?: bool,
     *     blocking?: bool,
     *     redis?: array{
     *       host?: string,
     *       port?: int,
     *       password?: string|null,
     *       database?: int,
     *       timeout?: float,
     *       persistent?: bool,
     *       persistent_id?: string|null
     *     },
     *     pdo?: array{
     *       dsn?: string,
     *       username?: string|null,
     *       password?: string|null,
     *       table?: string,
     *       options?: array<string, mixed>
     *     },
     *     file?: array{
     *       directory?: string|null
     *     },
     *     semaphore?: array<string, mixed>
     *   }
     * }
     */
    private array $config;

    /**
     * Default configuration.
     *
     * @var array<string, mixed>
     */
    private array $defaultConfig = [
        'tempDir' => null,
        'email'   => [
            'subject'             => 'Scheduled Job Output',
            'from'                => null,
            'body'                => 'Output from scheduled job execution',
            'transport'           => null,
            'ignore_empty_output' => true,
        ],
        'lock' => [
            'adapter'      => 'flock',
            'prefix'       => 'cron_lock_',
            'ttl'          => 300,
            'auto_release' => true,
            'blocking'     => false,
            'redis'        => [
                'host'          => '127.0.0.1',
                'port'          => 6379,
                'password'      => null,
                'database'      => 0,
                'timeout'       => 5.0,
                'persistent'    => false,
                'persistent_id' => null,
            ],
            'pdo' => [
                'dsn'      => null,
                'username' => null,
                'password' => null,
                'table'    => 'lock_keys',
                'options'  => [],
            ],
            'file' => [
                'directory' => null,
            ],
            'semaphore' => [],
        ],
    ];

    /**
     * Static lock factory instance (shared across scheduler instances).
     *
     * @var LockFactory|null
     */
    private static ?LockFactory $lockFactory = null;

    /**
     * Static lock store instance (shared across scheduler instances).
     *
     * @var SharedLockStoreInterface|PersistingStoreInterface|BlockingStoreInterface|null
     */
    private static SharedLockStoreInterface|PersistingStoreInterface|BlockingStoreInterface|null $lockStore = null;

    /**
     * Create new Scheduler instance.
     *
     * @param array<string, mixed> $config Configuration array
     */
    public function __construct(array $config = [])
    {
        $this->config = $this->mergeConfig($config);

        $this->validateConfig();

        // Set default tempDir if not provided
        if (empty($this->config['tempDir'])) {
            $this->config['tempDir'] = sys_get_temp_dir();
        }

        // Initialize lock factory if locking is enabled
        if (!empty($this->config['lock']['enabled'])) {
            $this->initializeLockFactory();
        }
    }

    /**
     * Merge user config with defaults.
     *
     * @param array<string, mixed> $config User configuration
     *
     * @return array<string, mixed>
     */
    private function mergeConfig(array $config): array
    {
        return array_replace_recursive($this->defaultConfig, $config);
    }

    /**
     * Validate configuration.
     *
     * @return void
     *
     * @throws InvalidArgumentException If configuration is invalid
     */
    private function validateConfig(): void
    {
        if (isset($this->config['lock']['enabled']) && $this->config['lock']['enabled']) {
            $adapter = $this->config['lock']['adapter'] ?? 'flock';
            $validAdapters = ['flock', 'redis', 'pdo', 'semaphore'];

            if (!in_array($adapter, $validAdapters, true)) {
                throw new InvalidArgumentException(
                    sprintf('Invalid lock adapter "%s". Valid adapters: %s', $adapter, implode(', ', $validAdapters))
                );
            }

            // Validate adapter-specific config
            switch ($adapter) {
                case 'redis':
                    if (empty($this->config['lock']['redis']['host'])) {
                        throw new InvalidArgumentException('Redis host is required for Redis lock adapter');
                    }

                    break;

                case 'pdo':
                    if (empty($this->config['lock']['pdo']['dsn'])) {
                        throw new InvalidArgumentException('PDO DSN is required for PDO lock adapter');
                    }

                    break;
            }
        }

        if (isset($this->config['lock']['ttl'])) {
            $ttl = $this->config['lock']['ttl'];
            if (!is_int($ttl) || $ttl <= 0) {
                throw new InvalidArgumentException('Lock TTL must be a positive integer');
            }
        }

        // Validate temp directory
        if (isset($this->config['tempDir']) && !is_dir($this->config['tempDir'])) {
            throw new InvalidArgumentException(
                sprintf('Temp directory "%s" does not exist', $this->config['tempDir'])
            );
        }
    }

    /**
     * Initialize the lock factory.
     *
     * @return void
     *
     * @throws RuntimeException If lock store cannot be created
     */
    private function initializeLockFactory(): void
    {
        if (self::$lockFactory === null || self::$lockStore === null) {
            self::$lockStore = $this->createLockStore();
            self::$lockFactory = new LockFactory(self::$lockStore);
        }
    }

    /**
     * Create the appropriate lock store based on configuration.
     *
     * @return SharedLockStoreInterface|PersistingStoreInterface|BlockingStoreInterface
     *
     * @throws RuntimeException If store cannot be created
     */
    private function createLockStore(): SharedLockStoreInterface|PersistingStoreInterface|BlockingStoreInterface
    {
        $adapter = $this->config['lock']['adapter'] ?? 'flock';

        try {
            switch ($adapter) {
                case 'redis':
                    return $this->createRedisStore();

                case 'pdo':
                    return $this->createPdoStore();

                case 'semaphore':
                    return new SemaphoreStore();

                case 'flock':
                default:
                    return $this->createFlockStore();
            }
        } catch (Exception $e) {
            throw new RuntimeException(
                sprintf('Failed to create %s lock store: %s', $adapter, $e->getMessage()),
                0,
                $e
            );
        }
    }

    /**
     * Create Redis lock store.
     *
     * @return RedisStore
     *
     * @throws RuntimeException If Redis connection fails
     */
    private function createRedisStore(): RedisStore
    {
        $config = $this->config['lock']['redis'] ?? [];

        $host = $config['host'] ?? '127.0.0.1';
        $port = (int) ($config['port'] ?? 6379);
        $timeout = (float) ($config['timeout'] ?? 5.0);
        $password = $config['password'] ?? null;
        $database = (int) ($config['database'] ?? 0);
        $persistent = (bool) ($config['persistent'] ?? false);
        $persistentId = $config['persistent_id'] ?? null;

        $redis = new Redis();

        // Connect with retry logic
        $maxRetries = 3;
        $connected = false;
        $lastException = null;

        for ($attempt = 1; $attempt <= $maxRetries; ++$attempt) {
            try {
                if ($persistent && $persistentId !== null) {
                    $connected = $redis->pconnect($host, $port, $timeout, $persistentId);
                } else {
                    $connected = $redis->connect($host, $port, $timeout);
                }

                if ($connected) {
                    break;
                }
            } catch (Exception $e) {
                $lastException = $e;
                if ($attempt < $maxRetries) {
                    usleep(100000 * $attempt); // Progressive backoff
                }
            }
        }

        if (!$connected) {
            throw new RuntimeException(
                sprintf('Failed to connect to Redis at %s:%d after %d attempts', $host, $port, $maxRetries),
                0,
                $lastException
            );
        }

        // Authenticate if needed
        if ($password !== null && $password !== '') {
            if (!$redis->auth($password)) {
                throw new RuntimeException('Redis authentication failed');
            }
        }

        // Select database
        if ($database > 0) {
            if (!$redis->select($database)) {
                throw new RuntimeException(sprintf('Failed to select Redis database %d', $database));
            }
        }

        return new RedisStore($redis);
    }

    /**
     * Create PDO lock store.
     *
     * @return PdoStore
     *
     * @throws RuntimeException If PDO connection fails
     */
    private function createPdoStore(): PdoStore
    {
        $config = $this->config['lock']['pdo'] ?? [];

        $dsn = $config['dsn'] ?? 'mysql:host=localhost;dbname=test';
        $username = $config['username'] ?? 'root';
        $password = $config['password'] ?? '';
        $options = $config['options'] ?? [];

        try {
            return new PdoStore($dsn, [
                'db_username' => $username,
                'db_password' => $password,
            ] + $options);
        } catch (Exception $e) {
            throw new RuntimeException(
                sprintf('Failed to create PDO connection: %s', $e->getMessage()),
                0,
                $e
            );
        }
    }

    /**
     * Create file-based (flock) lock store.
     *
     * @return FlockStore
     *
     * @throws RuntimeException If directory creation fails
     */
    private function createFlockStore(): FlockStore
    {
        $directory = $this->config['lock']['file']['directory'] ?? $this->config['tempDir'];

        // Ensure the directory exists
        if (!is_dir($directory) && !mkdir($directory, 0o777, true) && !is_dir($directory)) {
            throw new RuntimeException(sprintf('Failed to create lock directory: %s', $directory));
        }

        // Use a single lock file for all jobs (can be customized per job)
        $filePath = rtrim($directory, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'scheduler.lock';

        return new SchedulerFlockStore($filePath);
    }

    /**
     * Get scheduler configuration.
     *
     * @return array<string, mixed>
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * Create a lock for a specific job ID.
     *
     * @param string   $jobId The job identifier
     * @param int|null $ttl   Optional TTL override
     *
     * @return LockInterface
     *
     * @throws RuntimeException If lock cannot be created
     */
    public function createLock(string $jobId, ?int $ttl = null): LockInterface
    {
        if (self::$lockFactory === null) {
            $this->initializeLockFactory();
        }

        $lockId = $this->getLockId($jobId);
        $lockTtl = $ttl ?? $this->config['lock']['ttl'] ?? 300;
        $autoRelease = $this->config['lock']['auto_release'] ?? true;

        return self::$lockFactory->createLock($lockId, $lockTtl, $autoRelease);
    }

    /**
     * Check if a job is currently locked.
     *
     * @param string $jobId The job identifier
     *
     * @return bool True if locked, false otherwise
     */
    public function isJobLocked(string $jobId): bool
    {
        if (empty($this->config['lock']['enabled'])) {
            return false;
        }

        try {
            $testLock = $this->createLock($jobId, 1); // Very short TTL for testing

            if ($testLock->acquire(false)) {
                $testLock->release();

                return false;
            }

            return true;
        } catch (Exception $e) {
            error_log(sprintf('Failed to check lock status for job %s: %s', $jobId, $e->getMessage()));

            return false;
        }
    }

    /**
     * Get lock ID for a job.
     *
     * @param string $jobId The job identifier
     *
     * @return string
     */
    private function getLockId(string $jobId): string
    {
        $prefix = $this->config['lock']['prefix'] ?? 'cron_lock_';

        return $prefix . $jobId;
    }

    /**
     * Queue a job for execution.
     *
     * @param Job $job The job to queue
     *
     * @return void
     */
    private function queueJob(Job $job): void
    {
        $this->jobs[] = $job;
    }

    /**
     * Prioritise jobs by background capability.
     *
     * @return array<int, Job>
     */
    private function prioritiseJobs(): array
    {
        $background = [];
        $foreground = [];

        foreach ($this->jobs as $job) {
            if ($job->canRunInBackground()) {
                $background[] = $job;
            } else {
                $foreground[] = $job;
            }
        }

        return array_merge($background, $foreground);
    }

    /**
     * Get the queued jobs.
     *
     * @param DateTime|string|null $date Date to check for due jobs
     *
     * @return array<int, Job>
     */
    public function getQueuedJobs($date = null): array
    {
        if ($date === null) {
            return $this->prioritiseJobs();
        }

        $dueJobs = [];
        $dateTime = $date instanceof DateTime ? $date : new DateTime($date ?? 'now');

        foreach ($this->prioritiseJobs() as $job) {
            if ($job->isDue($dateTime)) {
                $dueJobs[] = $job;
            }
        }

        return $dueJobs;
    }

    /**
     * Queue a function execution.
     *
     * @param callable             $fn   The function to execute
     * @param array<string, mixed> $args Optional arguments to pass to the function
     * @param string|null          $id   Optional custom identifier
     *
     * @return Job
     */
    public function call(callable $fn, array $args = [], ?string $id = null): Job
    {
        $job = new Job($fn, $args, $id);
        $job->configure($this->config, $this);
        $this->queueJob($job);

        return $job;
    }

    /**
     * Queue a PHP script execution.
     *
     * @param string               $script The path to the PHP script to execute
     * @param string|null          $bin    Optional path to the PHP binary
     * @param array<string, mixed> $args   Optional arguments to pass to the PHP script
     * @param string|null          $id     Optional custom identifier
     *
     * @return Job
     *
     * @throws InvalidArgumentException If script is invalid
     */
    public function php(string $script, ?string $bin = null, array $args = [], ?string $id = null): Job
    {
        if (!file_exists($script)) {
            throw new InvalidArgumentException(
                sprintf('The script "%s" does not exist', $script)
            );
        }

        if (!is_readable($script)) {
            throw new InvalidArgumentException(
                sprintf('The script "%s" is not readable', $script)
            );
        }

        $phpBinary = $bin;
        if ($phpBinary === null) {
            $phpBinary = PHP_BINARY !== '' ? PHP_BINARY : '/usr/bin/php';
        } elseif (!file_exists($phpBinary)) {
            throw new InvalidArgumentException(
                sprintf('The PHP binary "%s" does not exist', $phpBinary)
            );
        }

        $command = sprintf('%s %s', $phpBinary, $script);
        $job = new Job($command, $args, $id);
        $job->configure($this->config, $this);
        $this->queueJob($job);

        return $job;
    }

    /**
     * Queue a raw shell command.
     *
     * @param string               $command The command to execute
     * @param array<string, mixed> $args    Optional arguments to pass to the command
     * @param string|null          $id      Optional custom identifier
     *
     * @return Job
     *
     * @throws InvalidArgumentException If command is empty
     */
    public function raw(string $command, array $args = [], ?string $id = null): Job
    {
        if (empty($command)) {
            throw new InvalidArgumentException('Command cannot be empty');
        }

        $job = new Job($command, $args, $id);
        $job->configure($this->config, $this);
        $this->queueJob($job);

        return $job;
    }

    /**
     * Run the scheduler.
     *
     * @param DateTime|null $runTime Optional specific time to run
     *
     * @return array<int, Job> Executed jobs
     */
    public function run(?DateTime $runTime = null): array
    {
        $jobs = $this->getQueuedJobs();
        $runTime ??= new DateTime('now');

        foreach ($jobs as $job) {
            if ($job->isDue($runTime)) {
                try {
                    $result = $job->run();
                    if ($result) {
                        $this->pushExecutedJob($job);
                    }
                } catch (Exception $e) {
                    $this->pushFailedJob($job, $e);
                }
            }
        }

        return $this->getExecutedJobs();
    }

    /**
     * Reset all collected data of last run.
     *
     * Call before run() if you call run() multiple times
     *
     * @return self
     */
    public function resetRun(): self
    {
        $this->executedJobs = [];
        $this->failedJobs = [];
        $this->outputSchedule = [];

        return $this;
    }

    /**
     * Add an entry to the scheduler verbose output.
     *
     * @param string $message The message to add
     *
     * @return void
     */
    private function addSchedulerVerboseOutput(string $message): void
    {
        $date = new DateTime('now');
        $timestamp = $date->format('c');
        $this->outputSchedule[] = sprintf('[%s] %s', $timestamp, $message);
    }

    /**
     * Push a successfully executed job.
     *
     * @param Job $job The executed job
     *
     * @return void
     */
    private function pushExecutedJob(Job $job): void
    {
        $this->executedJobs[] = $job;

        $compiled = $job->compile();
        $message = is_callable($compiled) ? 'Closure' : (string) $compiled;

        $this->addSchedulerVerboseOutput(
            sprintf('Executing job [%s]: %s', $job->getId(), $message)
        );
    }

    /**
     * Get the executed jobs.
     *
     * @return array<int, Job>
     */
    public function getExecutedJobs(): array
    {
        return $this->executedJobs;
    }

    /**
     * Push a failed job.
     *
     * @param Job       $job The failed job
     * @param Exception $e   The exception that caused the failure
     *
     * @return void
     */
    private function pushFailedJob(Job $job, Exception $e): void
    {
        $this->failedJobs[] = new FailedJob($job, $e);

        $compiled = $job->compile();
        $message = is_callable($compiled) ? 'Closure' : (string) $compiled;

        $this->addSchedulerVerboseOutput(
            sprintf('Failed job [%s]: %s - Error: %s', $job->getId(), $message, $e->getMessage())
        );
    }

    /**
     * Get the failed jobs.
     *
     * @return array<int, FailedJob>
     */
    public function getFailedJobs(): array
    {
        return $this->failedJobs;
    }

    /**
     * Get the scheduler verbose output.
     *
     * @param string $type Output type: 'text', 'html', or 'array'
     *
     * @return string|array<int, string>
     *
     * @throws InvalidArgumentException If invalid output type
     */
    public function getVerboseOutput(string $type = 'text')
    {
        switch ($type) {
            case 'text':
                return implode("\n", $this->outputSchedule);

            case 'html':
                return implode('<br>', array_map('htmlspecialchars', $this->outputSchedule));

            case 'array':
                return $this->outputSchedule;

            default:
                throw new InvalidArgumentException(
                    sprintf('Invalid output type "%s". Valid types: text, html, array', $type)
                );
        }
    }

    /**
     * Remove all queued jobs.
     *
     * @return self
     */
    public function clearJobs(): self
    {
        $this->jobs = [];

        return $this;
    }

    /**
     * Start a worker that runs continuously.
     *
     * @param array<int, int> $seconds Seconds when the scheduler should run (0-59)
     *
     * @return void
     *
     * @throws InvalidArgumentException If invalid second values
     */
    public function work(array $seconds = [0]): void
    {
        // Validate seconds
        foreach ($seconds as $second) {
            if ($second < 0 || $second > 59) {
                throw new InvalidArgumentException(
                    sprintf('Invalid second value %d. Must be between 0 and 59', $second)
                );
            }
        }

        while (true) {
            $currentSecond = (int) date('s');

            if (in_array($currentSecond, $seconds, true)) {
                $this->run();
                sleep(1);
            } else {
                usleep(100000); // Sleep for 100ms to reduce CPU usage
            }
        }
    }
}
