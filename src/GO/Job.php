<?php

declare(strict_types=1);

namespace GO;

use Cron\CronExpression;
use DateTime;
use Exception;
use InvalidArgumentException;
use Symfony\Component\Lock\LockInterface;

/**
 * Represents a scheduled job with execution logic and configuration.
 *
 * @author  Your Name
 * @license MIT
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
     * Command to execute.
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
    private ?CronExpression $executionTime = null;

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
     * This could prevent the job to run.
     * If true, the job will run (if due).
     *
     * @var bool
     */
    private bool $truthTest = true;

    /**
     * The output of the executed job.
     *
     * @var string|array<int, string>|null
     */
    private string|array $output;

    /**
     * The return code of the executed job.
     *
     * @var int
     */
    private int $returnCode = 0;

    /**
     * Files to write the output of the job.
     *
     * @var array<int, string>
     */
    private array $outputTo = [];

    /**
     * Email addresses where the output should be sent to.
     *
     * @var array<int, string>
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
     * Output mode for file writing.
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
     * Reference to the scheduler that manages this job.
     *
     * @var Scheduler|null
     */
    private ?Scheduler $scheduler = null;

    /**
     * Create a new Job instance.
     *
     * @param string|callable|array<mixed> $command The command or function to execute
     * @param array<string, mixed>         $args    Arguments for the command
     * @param string|null                  $id      Optional job identifier
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
     * @return string
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
     * @return string
     */
    public function getId(): string
    {
        return $this->id;
    }

    /**
     * Configure the job with scheduler settings.
     *
     * @param array<string, mixed> $config    Configuration array
     * @param Scheduler|null       $scheduler Reference to the scheduler
     *
     * @return self
     */
    public function configure(array $config = [], ?Scheduler $scheduler = null): self
    {
        // Store scheduler reference
        if ($scheduler !== null) {
            $this->scheduler = $scheduler;
        }

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

        return $this;
    }

    /**
     * Check if the Job is due to run.
     * It accepts as input a DateTime used to check if
     * the job is due. Defaults to job creation time.
     * It also defaults the execution time if not previously defined.
     *
     * @param DateTime|null $date Date to check against
     *
     * @return bool
     */
    public function isDue(?DateTime $date = null): bool
    {
        // The execution time is being defaulted if not defined
        if (!$this->executionTime) {
            $this->at('* * * * *');
        }

        $date ??= $this->creationTime;

        if ($this->executionYear && $this->executionYear !== $date->format('Y')) {
            return false;
        }

        return $this->executionTime->isDue($date);
    }

    /**
     * Check if the Job is overlapping using configured lock adapter.
     *
     * @return bool
     */
    public function isOverlapping(): bool
    {
        if (!$this->isLockable() || !$this->scheduler) {
            return true;
        }

        try {
            if ($this->whenOverlapping === null) {
                return false;
            }

            return (bool) call_user_func($this->whenOverlapping);
        } catch (Exception $e) {
            // Log error but don't fail the check
            error_log(sprintf('Failed to check overlap for job %s: %s', $this->id, $e->getMessage()));

            return false;
        }
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
     * @return bool
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
     * It prevents another instance of the same Job of
     * being executed if the previous is still running.
     * The job id is used as a filename for the lock file.
     *
     * @param string|null   $tempDir         The directory path for the lock files (legacy support)
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
     * Check if this job should use locking.
     *
     * @return bool
     */
    public function isLockable(): bool
    {
        return $this->lockable;
    }

    /**
     * Set the lock instance for this job.
     *
     * @param LockInterface $lock The lock instance
     *
     * @return void
     */
    public function setLock(LockInterface $lock): void
    {
        $this->lock = $lock;
    }

    /**
     * Compile the Job command.
     *
     * @return string|callable
     */
    public function compile(): string|callable
    {
        $compiled = $this->command;

        // If callable, return the function itself
        if (is_callable($compiled)) {
            return $compiled;
        }

        // Convert to string for shell commands
        $compiled = (string) $compiled;

        // Augment with any supplied arguments
        foreach ($this->args as $key => $value) {
            $compiled .= ' ' . escapeshellarg((string) $key);
            if ($value !== null) {
                $compiled .= ' ' . escapeshellarg((string) $value);
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
     * @param callable $fn Function that returns boolean
     *
     * @return self
     */
    public function when(callable $fn): self
    {
        $this->truthTest = (bool) $fn();

        return $this;
    }

    /**
     * Run the job.
     *
     * @return bool True if job executed successfully, false otherwise
     */
    public function run(): bool
    {
        // If the truthTest failed, don't run
        if ($this->truthTest !== true) {
            return false;
        }
        if (!$this->acquireLock()) {
            if (!$this->isOverlapping()) {
                return false;
            }

            error_log("Job '{$this->id}' is locked by another process, but overlapping allowed.");
            $this->releaseLock();
            if (!$this->acquireLock()) {
                return false;
            }
        }

        $compiled = $this->compile();

        if (is_callable($this->before)) {
            call_user_func($this->before);
        }

        try {
            if (is_callable($compiled)) {
                $this->output = $this->exec($compiled);
            } else {
                $output = [];
                exec($compiled, $output, $this->returnCode);
                $this->output = $output;
            }
            $this->releaseLock();
        } catch (Exception $e) {
            $this->returnCode = 1;
            $this->output = ['Error: ' . $e->getMessage()];
        } finally {
            $this->finalise();
        }

        return true;
    }

    /**
     * Acquire the job lock.
     *
     * @return bool True if lock acquired or not needed, false if already locked
     */
    private function acquireLock(): bool
    {
        if (!$this->isLockable() || !$this->scheduler) {
            return true;
        }

        try {
            $this->lock = $this->scheduler->createLock($this->id);

            if (!$this->lock->acquire()) {
                error_log("Job '{$this->id}' is locked by another process.");

                return false;
            }

            return true;
        } catch (Exception $e) {
            error_log('Error acquiring lock: ' . $e->getMessage());

            return false;
        }
    }

    /**
     * Release the job lock.
     *
     * @return void
     */
    private function releaseLock(): void
    {
        if ($this->lock instanceof LockInterface && $this->lock->isAcquired()) {
            $this->lock->release();
        }
    }

    /**
     * Execute a callable job.
     *
     * @param callable $fn Function to execute
     *
     * @return string Output from the function
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

        // Write to output files if specified
        foreach ($this->outputTo as $filename) {
            if ($outputBuffer !== false && $outputBuffer !== '') {
                file_put_contents($filename, $outputBuffer, $this->outputMode === 'a' ? FILE_APPEND : 0);
            }

            if ($returnData && is_string($returnData)) {
                file_put_contents($filename, $returnData, FILE_APPEND);
            }
        }

        $result = '';
        if ($outputBuffer !== false) {
            $result .= $outputBuffer;
        }
        if (is_string($returnData)) {
            $result .= $returnData;
        }

        return $result;
    }

    /**
     * Set the file/s where to write the output of the job.
     *
     * @param string|array<int, string> $filename File path(s) to write output
     * @param bool                      $append   Whether to append to existing files
     *
     * @return self
     */
    public function output(string|array $filename, bool $append = false): self
    {
        $this->outputTo = is_array($filename) ? $filename : [$filename];
        $this->outputMode = $append === false ? 'w' : 'a';

        return $this;
    }

    /**
     * Get the job output.
     *
     * @return string|array<int, string>|null
     */
    public function getOutput(): string|array
    {
        return $this->output;
    }

    /**
     * Get the job return code.
     *
     * @return int
     */
    public function getReturnCode(): int
    {
        return $this->returnCode;
    }

    /**
     * Set the emails where the output should be sent to.
     * The Job should be set to write output to a file
     * for this to work.
     *
     * @param string|array<int, string> $email Email address(es)
     *
     * @return self
     */
    public function email(string|array $email): self
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
     * Set function to be called before job execution
     * Job object is injected as a parameter to callable function.
     *
     * @param callable $fn Function to call before execution
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
     * By default this will force the job to run in foreground
     * because the output is injected as a parameter of this
     * function, but it could be avoided by passing true as a
     * second parameter. The job will run in background if it
     * meets all the other criteria.
     *
     * @param callable $fn              Function to call after execution
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
