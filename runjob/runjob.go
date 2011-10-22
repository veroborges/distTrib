// Package for running process and getting its result.
// Possible to also kill job

package runjob

import (
	"exec"
//	"fmt"
//	"io"
	"log"
	"os"
	"time"
)

// Useful functions

var verbosity int = 1

// Log result if verbosity level high enough
func Vlogf(level int, format string, v ...interface{}) {
	if level <= verbosity {
		log.Printf(format, v...)
	}	
}

func SetVerbose(level int) {
	verbosity = level
}

// Sleep for specified number of seconds
func Delay(seconds float32) {
	time.Sleep(int64(seconds * 1e9))
}


// Managing jobs

// Possible outcomes for a job
const (
	Running = iota
	Done
	Error
	Killed
	Timedout
	)

func StatusName(status int) string {
	names := map[int] string { Running: "Running", Done: "Done", Error: "Error", Killed : "Killed", Timedout: "Timedout" }
	return names[status]
}

type Job struct {
	Progname string
	Args []string
	Cmd *exec.Cmd
	Timelimit float32 // Time limit in seconds  (0.0 means run indefinitely)
	Status int // One of the above statuses
	Result string
	Logfile *os.File
	Timechan chan bool
	Donechan chan bool
	Killchan chan bool
	// Filled in by executing program
	err os.Error
	bout []byte
}

func NewJob(progname string, timelimit float32, logfile *os.File, args ...string) *Job {
	j := new(Job)
	j.Progname = progname
	j.Args = args
	j.Timelimit = timelimit
	j.Status = Running
	j.Result = ""
	j.Logfile = logfile
	j.Timechan = make(chan bool, 1)
	j.Donechan = make(chan bool, 1)
	j.Killchan = make(chan bool, 1)
	go j.start()
	if timelimit > 0.0 {
		go j.runtimeout()
	}
	return j
}

// Set up timeout
func (j *Job) runtimeout() {
	Delay(j.Timelimit)
	j.Timechan <- true
}

// Innermost program
func (j *Job) dojob (donechan chan bool) {
	j.Cmd = exec.Command(j.Progname, j.Args...)
	Vlogf(1, "Running %s with arguments %v\n", j.Progname, j.Args)
	if j.Logfile != nil {
		j.Cmd.Stderr = j.Logfile
		Vlogf(3, "Logging of stderr for %s enabled\n", j.Progname)
		j.bout, j.err = j.Cmd.Output()
	} else {
		j.bout, j.err = j.Cmd.CombinedOutput()
	}
	donechan <- true
}

// Next level: enable kill
func (j *Job) start() {
	donechan := make(chan bool, 1)
	go j.dojob(donechan)
	for j.Status == Running {
		select {
		case <- donechan:
			if j.err == nil {
				j.Status = Done
				j.Result = string(j.bout)
				Vlogf(2, "Completed %s.  Result = %s\n",
					j.Progname, j.Result)
			} else {
				j.Status = Error
				j.Result = j.err.String()
				Vlogf(2, "Error for %s.  Message = %s\n",
					j.Progname, j.Result)
			}
		case <- j.Killchan:
			if j.Logfile != nil {
				// Close logfile before killing job
				j.Logfile.Close()
			}
			err := j.Cmd.Process.Kill()
			if err == nil {
				Vlogf(2, "Killed %s\n", j.Progname)
				j.Status = Killed
				j.Result = "Killed"
			} else {
				Vlogf(1, "Failed attempt to kill %s.  Message %s\n",
					j.Progname, err.String())
				j.Status = Error
				j.Result = err.String()
			}
		case <- j.Timechan:
			j.Status = Timedout
			j.Result = "Timed limit exceeded"
			Vlogf(2, "Program %s timed out after %.2f secs\n",
				j.Progname, j.Timelimit)
		}
		
	}
	j.Donechan <- true
}

func (j *Job) Kill() {
	j.Killchan <- true
}

func (j *Job) GetResult() (string, int) { 
	<- j.Donechan
	return j.Result, j.Status
}

func (j *Job) Running() bool {
	return j.Status == Running
}
