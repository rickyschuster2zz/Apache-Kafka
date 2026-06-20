package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Errors
var (
	ErrCommitFailed      = errors.New("CommitFailedException: Commit failed because the consumer is no longer part of the group or the partition is no longer assigned")
	ErrFencedMemberEpoch = errors.New("FencedMemberEpochException: The member epoch has changed due to a rebalance")
)

// TopicPartition represents a partition of a topic
type TopicPartition struct {
	Topic     string
	Partition int
}

// SubscriptionState tracks the current partition assignment
type SubscriptionState struct {
	mu        sync.RWMutex
	assigned  map[TopicPartition]bool
}

func NewSubscriptionState() *SubscriptionState {
	return &SubscriptionState{
		assigned: make(map[TopicPartition]bool),
	}
}

func (s *SubscriptionState) Assign(partitions []TopicPartition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assigned = make(map[TopicPartition]bool)
	for _, tp := range partitions {
		s.assigned[tp] = true
	}
}

func (s *SubscriptionState) Revoke(partitions []TopicPartition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tp := range partitions {
		delete(s.assigned, tp)
	}
}

func (s *SubscriptionState) IsAssigned(tp TopicPartition) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.assigned[tp]
}

// CommitCallback is invoked when a commit completes or fails
type CommitCallback func(offsets map[TopicPartition]int64, err error)

// PendingCommit represents an in-flight commit request
type PendingCommit struct {
	Offsets    map[TopicPartition]int64
	Generation int
	Callback   CommitCallback
	Done       chan struct{}
}

// ConsumerCoordinator coordinates commits and rebalances
type ConsumerCoordinator struct {
	mu                sync.Mutex
	subscriptionState *SubscriptionState
	generation        int
	pendingCommits    []*PendingCommit
}

func NewConsumerCoordinator(subState *SubscriptionState) *ConsumerCoordinator {
	return &ConsumerCoordinator{
		subscriptionState: subState,
		generation:        1,
		pendingCommits:    make([]*PendingCommit, 0),
	}
}

// CommitAsync initiates an asynchronous commit
func (c *ConsumerCoordinator) CommitAsync(offsets map[TopicPartition]int64, callback CommitCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Client-Side Fencing: Check if partitions are still assigned
	for tp := range offsets {
		if !c.subscriptionState.IsAssigned(tp) {
			if callback != nil {
				go callback(offsets, ErrCommitFailed)
			}
			return
		}
	}

	commit := &PendingCommit{
		Offsets:    offsets,
		Generation: c.generation,
		Callback:   callback,
		Done:       make(chan struct{}),
	}
	c.pendingCommits = append(c.pendingCommits, commit)

	// Simulate broker response asynchronously
	go c.sendCommitRequest(commit)
}

func (c *ConsumerCoordinator) sendCommitRequest(commit *PendingCommit) {
	// Simulate network delay
	time.Sleep(100 * time.Millisecond)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the commit has already been completed/cancelled (e.g. during revocation)
	select {
	case <-commit.Done:
		return
	default:
	}

	// Remove from pending list
	c.removePendingCommit(commit)

	// Generation Validation & Client-Side Fencing
	if commit.Generation != c.generation {
		if commit.Callback != nil {
			commit.Callback(commit.Offsets, ErrFencedMemberEpoch)
		}
		return
	}

	for tp := range commit.Offsets {
		if !c.subscriptionState.IsAssigned(tp) {
			if commit.Callback != nil {
				commit.Callback(commit.Offsets, ErrCommitFailed)
			}
			return
		}
	}

	// Success
	if commit.Callback != nil {
		commit.Callback(commit.Offsets, nil)
	}
}

func (c *ConsumerCoordinator) removePendingCommit(commit *PendingCommit) {
	for i, pc := range c.pendingCommits {
		if pc == commit {
			c.pendingCommits = append(c.pendingCommits[:i], c.pendingCommits[i+1:]...)
			break
		}
	}
}

// RevokePartitions handles partition revocation during a rebalance
func (c *ConsumerCoordinator) RevokePartitions(partitions []TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Revoke in subscription state
	c.subscriptionState.Revoke(partitions)

	// Increment generation to fence any future/in-flight commits
	c.generation++

	// Async Commit Cleanup: Complete outstanding commits for revoked partitions immediately
	var remainingCommits []*PendingCommit
	for _, commit := range c.pendingCommits {
		hasRevokedPartition := false
		for tp := range commit.Offsets {
			for _, revoked := range partitions {
				if tp == revoked {
					hasRevokedPartition = true
					break
				}
			}
		}

		if hasRevokedPartition {
			close(commit.Done)
			if commit.Callback != nil {
				// Callback Safety: Invoke callback with ErrCommitFailed immediately
				go commit.Callback(commit.Offsets, ErrCommitFailed)
			}
		} else {
			remainingCommits = append(remainingCommits, commit)
		}
	}
	c.pendingCommits = remainingCommits
}

func main() {
	fmt.Println("Starting Kafka Consumer Coordinator simulation...")

	subState := NewSubscriptionState()
	coordinator := NewConsumerCoordinator(subState)

	tp1 := TopicPartition{Topic: "test-topic", Partition: 0}
	tp2 := TopicPartition{Topic: "test-topic", Partition: 1}

	// Assign partitions
	subState.Assign([]TopicPartition{tp1, tp2})
	fmt.Println("Assigned partitions: tp1, tp2")

	var wg sync.WaitGroup
	wg.Add(2)

	// 1. Commit for tp1 (will be revoked before broker responds)
	fmt.Println("Initiating async commit for tp1...")
	coordinator.CommitAsync(map[TopicPartition]int64{tp1: 100}, func(offsets map[TopicPartition]int64, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("Commit callback for tp1: Failed as expected: %v\n", err)
		} else {
			fmt.Println("Commit callback for tp1: Succeeded (ERROR: Should have failed due to revocation!)")
		}
	})

	// 2. Commit for tp2 (will remain assigned)
	fmt.Println("Initiating async commit for tp2...")
	coordinator.CommitAsync(map[TopicPartition]int64{tp2: 200}, func(offsets map[TopicPartition]int64, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("Commit callback for tp2: Failed: %v\n", err)
		} else {
			fmt.Println("Commit callback for tp2: Succeeded successfully")
		}
	})

	// Simulate rebalance starting: revoke tp1
	time.Sleep(30 * time.Millisecond)
	fmt.Println("Rebalance triggered: Revoking tp1...")
	coordinator.RevokePartitions([]TopicPartition{tp1})

	wg.Wait()
	fmt.Println("Simulation finished.")
}