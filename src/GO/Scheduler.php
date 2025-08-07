<?php

declare(strict_types=1);

namespace GO;

use DateTime;
use Exception;
use InvalidArgumentException;

/**
 * Job Scheduler with centralized configuration.
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
     *     enabled?: bool,
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
     *     memcached?: array{
     *       host?: string,
     *       port?: int,
     *       options?: array<int, mixed>
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
            'adapter'      => 'file',
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
            'memcached' => [
                'host'    => '127.0.0.1',
                'port'    => 11211,
                'options' => [],
            ],
            'file' => [
                'directory' => null,
            ],
            'semaphore' => [],
        ],
    ];

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
            $adapter = $this->config['lock']['adapter'] ?? 'file';
            $validAdapters = ['file', 'redis', 'pdo', 'memcached', 'semaphore'];

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

                case 'memcached':
                    if (empty($this->config['lock']['memcached']['host'])) {
                        throw new InvalidArgumentException('Memcached host is required for Memcached lock adapter');
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
    public function getQueuedJobs(DateTime|string|null $date = null): array
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
     * @param callable     $fn   The function to execute
     * @param array<mixed> $args Optional arguments to pass to the function
     * @param string|null  $id   Optional custom identifier
     *
     * @return Job
     */
    public function call(callable $fn, array $args = [], ?string $id = null): Job
    {
        $job = new Job($fn, $args, $id);
        $job->configure($this->config);
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
        $job->configure($this->config);
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
     */
    public function raw(string $command, array $args = [], ?string $id = null): Job
    {
        if (empty($command)) {
            throw new InvalidArgumentException('Command cannot be empty');
        }

        $job = new Job($command, $args, $id);
        $job->configure($this->config);
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
        $message = is_callable($compiled) ? 'Closure' : $compiled;

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
        $message = is_callable($compiled) ? 'Closure' : $compiled;

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
    public function getVerboseOutput(string $type = 'text'): array|string
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
