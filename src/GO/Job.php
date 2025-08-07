<?php

declare(strict_types=1);

namespace GO;

use Cron\CronExpression;
use DateTime;
use Exception;
use InvalidArgumentException;
use Memcached;
use Redis;
use RuntimeException;
use Symfony\Component\Lock\BlockingStoreInterface;
use Symfony\Component\Lock\Key;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\LockInterface;
use Symfony\Component\Lock\PersistingStoreInterface;
use Symfony\Component\Lock\SharedLockStoreInterface;
use Symfony\Component\Lock\Store\FlockStore;
use Symfony\Component\Lock\Store\MemcachedStore;
use Symfony\Component\Lock\Store\PdoStore;
use Symfony\Component\Lock\Store\RedisStore;
use Symfony\Component\Lock\Store\SemaphoreStore;

/**
 * Job class for scheduling and executing tasks with locking mechanism.
 *
 * This class provides functionality to create, schedule, and execute jobs
 * with various features like overlap prevention, output handling, and email notifications.
 *
 * @author  Your Name
 *
 * @version 1.0.0
 *
 * @since   1.0.0
 */
class Job
{
    use Traits\Interval;
    use Traits\Mailer;

    /**
     * Job identifier.
     *
     * @var string
     */
    private string $id;

    /**
     * Command to execute (string, callable, or array).
     *
     * @var string|callable|array<mixed>
     */
    private $command;

    /**
     * Arguments to be passed to the command.
     *
     * @var array<string, mixed>
     */
    private array $args = [];

    /**
     * Defines if the job should run in background.
     *
     * @var bool
     */
    private bool $runInBackground = true;

    /**
     * Creation time.
     *
     * @var DateTime
     */
    private DateTime $creationTime;

    /**
     * Job schedule time.
     *
     * @var CronExpression|null
     */
    private ?CronExpression $executionTime;

    /**
     * Job schedule year.
     *
     * @var string|null
     */
    private ?string $executionYear = null;

    /**
     * Temporary directory path for lock files to prevent overlapping.
     *
     * @var string
     */
    private string $tempDir;

    /**
     * Path to the lock file.
     *
     * @var string|null
     */
    private ?string $lockFile = null;

    /**
     * This could prevent the job to run.
     * If true, the job will run (if due).
     *
     * @var bool
     */
    private bool $truthTest = true;

    /**
     * The output of the executed job.
     *
     * @var string|array<string>|null
     */
    private string|array|null $output;

    /**
     * The return code of the executed job.
     *
     * @var int
     */
    private int $returnCode = 0;

    /**
     * Files to write the output of the job.
     *
     * @var array<string>
     */
    private array $outputTo = [];

    /**
     * Email addresses where the output should be sent to.
     *
     * @var array<string>
     */
    private array $emailTo = [];

    /**
     * Configuration for email sending.
     *
     * @var array<string, mixed>
     */
    private array $emailConfig = [];

    /**
     * A function to execute before the job is executed.
     *
     * @var callable|null
     */
    private $before;

    /**
     * A function to execute after the job is executed.
     *
     * @var callable|null
     */
    private $after;

    /**
     * A function to ignore an overlapping job.
     * If true, the job will run also if it's overlapping.
     *
     * @var callable|null
     */
    private $whenOverlapping;

    /**
     * Output mode for file writing ('w' for write, 'a' for append).
     *
     * @var string
     */
    private string $outputMode = 'w';

    /**
     * Whether this job should use locking.
     *
     * @var bool
     */
    private bool $lockable = false;

    /**
     * The Symfony Lock instance.
     *
     * @var LockInterface|null
     */
    private ?LockInterface $lock = null;

    /**
     * Lock configuration from scheduler.
     *
     * @var array<string, mixed>
     */
    private array $lockConfig = [];

    /**
     * Static lock factory instance (shared across jobs).
     *
     * @var LockFactory|null
     */
    private static ?LockFactory $lockFactory = null;

    /**
     * Static lock store instance (shared across jobs).
     *
     * @var SharedLockStoreInterface|PersistingStoreInterface|BlockingStoreInterface|null
     */
    private static SharedLockStoreInterface|PersistingStoreInterface|BlockingStoreInterface|null $lockStore = null;

    /**
     * Create a new Job instance.
     *
     * @param string|callable|array<mixed> $command The command or function to execute
     * @param array<string, mixed>         $args    Arguments for the command
     * @param string|null                  $id      Optional job identifier
     *
     * @throws InvalidArgumentException When command is invalid
     */
    public function __construct(string|callable|array $command, array $args = [], ?string $id = null)
    {
        $this->command = $command;
        $this->args = $args;
        $this->creationTime = new DateTime('now');
        $this->tempDir = sys_get_temp_dir();

        // Generate ID if not provided
        if ($id !== null) {
            $this->id = $id;
        } else {
            $this->id = $this->generateId($command);
        }
    }

    /**
     * Generate job ID based on command.
     *
     * @param string|callable|array<mixed> $command The command
     *
     * @return string The generated unique identifier
     */
    private function generateId(string|callable|array $command): string
    {
        if (is_string($command)) {
            return md5($command);
        }

        if (is_array($command)) {
            return md5(serialize($command));
        }

        if (is_object($command)) {
            return spl_object_hash($command);
        }

        return md5(uniqid('job_', true));
    }

    /**
     * Get the Job id.
     *
     * @return string The job identifier
     */
    public function getId(): string
    {
        return $this->id;
    }

    /**
     * Configure the job with scheduler settings.
     *
     * @param array<string, mixed> $config Configuration array
     *
     * @return self
     *
     * @throws InvalidArgumentException When temp directory doesn't exist
     */
    public function configure(array $config = []): self
    {
        // Configure email settings
        if (isset($config['email']) && is_array($config['email'])) {
            $this->emailConfig = $config['email'];
        }

        // Configure temp directory
        if (isset($config['tempDir']) && is_string($config['tempDir'])) {
            if (!is_dir($config['tempDir'])) {
                throw new InvalidArgumentException(
                    sprintf('Temp directory "%s" does not exist', $config['tempDir'])
                );
            }
            $this->tempDir = $config['tempDir'];
        }

        // Configure lock settings
        if (isset($config['lock']) && is_array($config['lock'])) {
            $this->lockConfig = $config['lock'];
        }

        return $this;
    }

    /**
     * Check if the Job is due to run.
     *
     * It accepts as input a DateTime used to check if
     * the job is due. Defaults to job creation time.
     * It also defaults the execution time if not previously defined.
     *
     * @param DateTime|null $date Date to check against, defaults to creation time
     *
     * @return bool True if job is due to run
     */
    public function isDue(?DateTime $date = null): bool
    {
        // The execution time is being defaulted if not defined
        if (!$this->executionTime) {
            $this->at('* * * * *');
        }

        $date = $date !== null ? $date : $this->creationTime;

        if ($this->executionYear && $this->executionYear !== $date->format('Y')) {
            return false;
        }

        return $this->executionTime->isDue($date);
    }

    /**
     * Check if the Job is overlapping using configured lock adapter.
     *
     * @return bool True if job is overlapping with another instance
     */
    public function isOverlapping(): bool
    {
        if (!$this->lockable || empty($this->lockConfig['enabled'])) {
            return false;
        }

        try {
            $factory = $this->getLockFactory();
            $lockId = $this->getLockId();

            // Try to acquire a test lock with minimal TTL
            $testLock = $factory->createLock($lockId, 0.1);

            if ($testLock->acquire(false)) {
                $testLock->release();

                return false;
            }

            return true;
        } catch (Exception $e) {
            // Log error but don't fail the check
            error_log(sprintf('Failed to check overlap for job %s: %s', $this->id, $e->getMessage()));

            return false;
        }
    }

    /**
     * Get lock ID for this job.
     *
     * @return string The lock identifier
     */
    private function getLockId(): string
    {
        $prefix = $this->lockConfig['prefix'] ?? 'cron_lock_';

        return $prefix . $this->id;
    }

    /**
     * Get or create the lock factory.
     *
     * @return LockFactory The lock factory instance
     *
     * @throws RuntimeException If lock store cannot be created
     */
    private function getLockFactory(): LockFactory
    {
        if (self::$lockFactory === null || self::$lockStore === null) {
            self::$lockStore = $this->createLockStore();
            self::$lockFactory = new LockFactory(self::$lockStore);
        }

        return self::$lockFactory;
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
        $adapter = $this->lockConfig['adapter'] ?? 'flock';

        try {
            switch ($adapter) {
                case 'redis':
                    return $this->createRedisStore();

                case 'pdo':
                    return $this->createPdoStore();

                case 'memcached':
                    return $this->createMemcachedStore();

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
     * @return RedisStore The Redis lock store
     *
     * @throws RuntimeException If Redis connection fails
     */
    private function createRedisStore(): RedisStore
    {
        $config = $this->lockConfig['redis'] ?? [];

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
     * @return PdoStore The PDO lock store
     *
     * @throws RuntimeException If PDO connection fails
     */
    public function createPdoStore(): PdoStore
    {
        $dsn = $this->lockConfig['pdo']['dsn'] ?? 'mysql:host=localhost;dbname=test';
        $username = $this->lockConfig['pdo']['username'] ?? 'root';
        $password = $this->lockConfig['pdo']['password'] ?? '';

        return new PdoStore($dsn, ['db_username' => $username, 'db_password' => $password]);
    }

    /**
     * Create Memcached lock store.
     *
     * @return MemcachedStore The Memcached lock store
     *
     * @throws InvalidArgumentException If Memcached server configuration is invalid
     */
    public function createMemcachedStore(): MemcachedStore
    {
        $memcached = new Memcached();
        $servers = $this->lockConfig['memcached']['servers'] ?? [['127.0.0.1', 11211]];
        foreach ($servers as $server) {
            if (is_array($server) && count($server) >= 2) {
                $memcached->addServer($server[0], $server[1]);
            } else {
                throw new InvalidArgumentException('Invalid Memcached server configuration');
            }
        }

        return new MemcachedStore($memcached);
    }

    /**
     * Create Flock lock store.
     *
     * @return FlockStore The Flock lock store
     *
     * @throws RuntimeException If temp directory creation fails
     */
    public function createFlockStore(): FlockStore
    {
        // Default file store uses system temp directory
        $filePath = $this->tempDir . DIRECTORY_SEPARATOR . 'lock_' . $this->id . '.lock';

        // Ensure the directory exists
        if (!is_dir($this->tempDir) && !mkdir($this->tempDir, 0o777, true) && !is_dir($this->tempDir)) {
            throw new RuntimeException(sprintf('Failed to create temp directory: %s', $this->tempDir));
        }

        return new FlockStore($filePath);
    }

    /**
     * Force the Job to run in foreground.
     *
     * @return self
     */
    public function inForeground(): self
    {
        $this->runInBackground = false;

        return $this;
    }

    /**
     * Check if the Job can run in background.
     *
     * @return bool True if job can run in background
     */
    public function canRunInBackground(): bool
    {
        if (is_callable($this->command) || $this->runInBackground === false) {
            return false;
        }

        return true;
    }

    /**
     * This will prevent the Job from overlapping.
     *
     * It prevents another instance of the same Job of
     * being executed if the previous is still running.
     * The job id is used as a filename for the lock file.
     *
     * @param string|null   $tempDir         The directory path for the lock files
     * @param callable|null $whenOverlapping A callback to ignore job overlapping
     *
     * @return self
     */
    public function onlyOne(?string $tempDir = null, ?callable $whenOverlapping = null): self
    {
        $this->lockable = true;

        if ($whenOverlapping !== null) {
            $this->whenOverlapping = $whenOverlapping;
        }

        // Legacy support for tempDir parameter
        if ($tempDir !== null && is_dir($tempDir)) {
            $this->tempDir = $tempDir;
        }

        return $this;
    }

    /**
     * Compile the Job command.
     *
     * @return string|callable The compiled command
     */
    public function compile()
    {
        $compiled = $this->command;

        // If callable, return the function itself
        if (is_callable($compiled)) {
            return $compiled;
        }

        // Augment with any supplied arguments
        foreach ($this->args as $key => $value) {
            $compiled .= ' ' . escapeshellarg($key);
            if ($value !== null) {
                $compiled .= ' ' . escapeshellarg($value);
            }
        }

        // Add the boilerplate to redirect the output to file/s
        if (count($this->outputTo) > 0) {
            if (file_exists('/dev/null')) {
                // linux systems
                $compiled .= ' | tee ';
                $compiled .= $this->outputMode === 'a' ? '-a ' : '';
            } else {
                // windows systems
                $compiled .= ' ';
                $compiled .= $this->outputMode === 'a' ? '>> ' : '> ';
            }
            foreach ($this->outputTo as $file) {
                $compiled .= $file . ' ';
            }

            $compiled = trim($compiled);
        }

        // Add boilerplate to remove lockfile after execution
        if ($this->lockFile) {
            if (file_exists('/dev/null')) {
                // linux systems
                $compiled .= '; rm ' . $this->lockFile;
            } else {
                // windows systems
                $compiled .= ' & del ' . $this->lockFile;
            }
        }

        // Add boilerplate to run in background
        if ($this->canRunInBackground()) {
            // Parentheses are need execute the chain of commands in a subshell
            // that can then run in background
            if (file_exists('/dev/null')) {
                // linux systems
                $compiled = '(' . $compiled . ') > /dev/null 2>&1 &';
            } else {
                // windows systems
                $compiled = '(' . $compiled . ') > NUL 2>&1';
            }
        }

        return trim($compiled);
    }

    /**
     * Truth test to define if the job should run if due.
     *
     * @param callable $fn The truth test function
     *
     * @return self
     */
    public function when(callable $fn): self
    {
        $this->truthTest = $fn();

        return $this;
    }

    /**
     * Run the job.
     *
     * @return bool True if job was executed, false if skipped
     */
    public function run(): bool
    {
        // If the truthTest failed, don't run
        if ($this->truthTest !== true) {
            return false;
        }

        // If overlapping, don't run
        if ($this->lockable && !$this->createLock()) {
            return false;
        }

        $compiled = $this->compile();

        if (is_callable($this->before)) {
            call_user_func($this->before);
        }

        if (is_callable($compiled)) {
            $this->output = $this->exec($compiled);
        } else {
            exec($compiled, $this->output, $this->returnCode);
        }

        $this->finalise();

        $this->removeLock();

        return true;
    }

    /**
     * Create the job lock file.
     *
     * @return bool True if lock was created successfully
     */
    private function createLock(): bool
    {
        try {
            $factory = $this->getLockFactory();

            $lockId = new Key($this->getLockId());

            $this->lock = $factory->createLockFromKey($lockId, 300, true); // TTL de 5 minutos

            if (!$this->lock->acquire()) {
                error_log("Job '{$this->id}' is locked by another process.");

                return false;
            }

            return true;
        } catch (Exception $e) {
            error_log('Error creating lock: ' . $e->getMessage());

            return false;
        }
    }

    /**
     * Remove the job lock file.
     *
     * @return void
     */
    private function removeLock(): void
    {
        if ($this->lock instanceof LockInterface && $this->lock->isAcquired()) {
            $this->lock->release();
        }
    }

    /**
     * Execute a callable job.
     *
     * @param callable $fn The function to execute
     *
     * @return string The execution output
     *
     * @throws Exception If execution fails
     */
    private function exec(callable $fn): string
    {
        ob_start();

        try {
            $returnData = call_user_func_array($fn, $this->args);
        } catch (Exception $e) {
            ob_end_clean();

            throw $e;
        }

        $outputBuffer = ob_get_clean();

        foreach ($this->outputTo as $filename) {
            if ($outputBuffer) {
                file_put_contents($filename, $outputBuffer, $this->outputMode === 'a' ? FILE_APPEND : 0);
            }

            if ($returnData) {
                file_put_contents($filename, $returnData, FILE_APPEND);
            }
        }

        return $outputBuffer . (is_string($returnData) ? $returnData : '');
    }

    /**
     * Set the file/s where to write the output of the job.
     *
     * @param string|array<string> $filename Single filename or array of filenames
     * @param bool                 $append   Whether to append to file or overwrite
     *
     * @return self
     */
    public function output(string $filename, bool $append = false): self
    {
        $this->outputTo = is_array($filename) ? $filename : [$filename];
        $this->outputMode = $append === false ? 'w' : 'a';

        return $this;
    }

    /**
     * Get the job output.
     *
     * @return string|array<string>|null The job output
     */
    public function getOutput(): array|string|null
    {
        return $this->output;
    }

    /**
     * Set the emails where the output should be sent to.
     *
     * The Job should be set to write output to a file
     * for this to work.
     *
     * @param string|array<string> $email Single email or array of emails
     *
     * @return self
     *
     * @throws InvalidArgumentException If email parameter is invalid
     */
    public function email(string $email): self
    {
        if (!is_string($email) && !is_array($email)) {
            throw new InvalidArgumentException('The email can be only string or array');
        }

        $this->emailTo = is_array($email) ? $email : [$email];

        // Force the job to run in foreground
        $this->inForeground();

        return $this;
    }

    /**
     * Finalise the job after execution.
     *
     * @return void
     */
    private function finalise(): void
    {
        // Send output to email
        $this->emailOutput();

        // Call any callback defined
        if (is_callable($this->after)) {
            call_user_func($this->after, $this->output, $this->returnCode);
        }
    }

    /**
     * Email the output of the job, if any.
     *
     * @return bool True if email was sent, false otherwise
     */
    private function emailOutput(): bool
    {
        if (!count($this->outputTo) || !count($this->emailTo)) {
            return false;
        }

        if (
            isset($this->emailConfig['ignore_empty_output'])
            && $this->emailConfig['ignore_empty_output'] === true
            && empty($this->output)
        ) {
            return false;
        }

        $this->sendToEmails($this->outputTo);

        return true;
    }

    /**
     * Set function to be called before job execution.
     *
     * Job object is injected as a parameter to callable function.
     *
     * @param callable $fn The function to call before execution
     *
     * @return self
     */
    public function before(callable $fn): self
    {
        $this->before = $fn;

        return $this;
    }

    /**
     * Set a function to be called after job execution.
     *
     * By default this will force the job to run in foreground
     * because the output is injected as a parameter of this
     * function, but it could be avoided by passing true as a
     * second parameter. The job will run in background if it
     * meets all the other criteria.
     *
     * @param callable $fn              The function to call after execution
     * @param bool     $runInBackground Whether to allow background execution
     *
     * @return self
     */
    public function then(callable $fn, bool $runInBackground = false): self
    {
        $this->after = $fn;

        // Force the job to run in foreground
        if ($runInBackground === false) {
            $this->inForeground();
        }

        return $this;
    }
}
