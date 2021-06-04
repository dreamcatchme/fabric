package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	RaftCount      = 5                       // 节点个数
	NotVote        = -1                      // 不投票
	NotLeader      = -1                      // 无领导者
	Follower       = 0                       // 跟随者
	Candidate      = 1                       // 候选者
	Leader         = 2                       // 领导者
	MaxTimeout     = 3000                    // 最大超时时间
	MinTimeout     = 1500                    // 最小超时时间
	Timeout        = MaxTimeout - MinTimeout // 超时时间
	IsGtNodeNumber = RaftCount / 2           // 大于选举节点的数量
	NotTerm        = 0                       // 无任期
)

// raft raft对象
type raft struct {
	mu              sync.Mutex // 锁
	me              int        // 节点编号
	currentTerm     int        // 当前任期
	voteFor         int        // 为哪个节点投票
	state           int        // 状态：0为跟随者，1为候选人，2为选举者
	timeout         int        // 超时设置
	currentLeader   int        // 当前的节点的领导是谁
	lastMessageTime int64      // 发送的最后的时间
	message         chan bool  // 节点间发送信息的通道
	electChan       chan bool  // 选举通道
	heartBeat       chan bool  // 心跳机制的通道
	heartBeatReturn chan bool  // 返回心跳机制的通道
}

var LeaderNode = leader{id: NotLeader, term: NotTerm, vote: 0}
var isBirthLeader = false

// leader 主节点对象
type leader struct {
	term int // 任期
	id   int //编号
	vote int //投票数
}

func (rf *raft) setTerm(term int) {
	rf.currentTerm = term
}

func getStatus(status int) (stu string) {
	switch status {
	case Follower:
		stu = "跟随者"
	case Candidate:
		stu = "候选者"
	case Leader:
		stu = "领导者"
	default:
		stu = ""
	}
	return
}

func randRange() int64 {
	return rand.Int63n(Timeout) + MinTimeout
}

// election 选举函数
func (rf *raft) election() {
	for {
		timeout := randRange()
		rf.lastMessageTime = millisecond()
		select {
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			fmt.Printf("当前%d节点状态为：%s\n", rf.me, getStatus(rf.state))
		}
		for !isBirthLeader {
			fmt.Println("===========选举中.....===============")
			isBirthLeader = rf.electionLeader(&LeaderNode)
			fmt.Println("===========选举成功===============")
		}
	}
}

// initialize 初始化函数
func initialize(me int) {
	rf := &raft{
		me:              me,
		state:           Follower,
		voteFor:         NotVote,
		timeout:         Timeout,
		currentLeader:   NotLeader,
		message:         make(chan bool),
		electChan:       make(chan bool),
		heartBeat:       make(chan bool),
		heartBeatReturn: make(chan bool),
	}
	rf.setTerm(NotTerm)
	rand.Seed(time.Now().UnixNano())
	go rf.election()
	go rf.sendLeaderHeartBeat()
}

func millisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (rf *raft) electionLeader(lead *leader) bool {
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	fmt.Println("开始选举领导者")
	for i := 0; i < RaftCount; i++ {
		if i != rf.me && lead.id == NotLeader {
			go func() {
				rf.electChan <- true
			}()
		}
	}
	vote := 1
	for i := 0; i < RaftCount; i++ {
		select {
		case ok := <-rf.electChan:
			if ok {
				vote++
				if vote > IsGtNodeNumber {
					rf.mu.Lock()
					rf.becomeLeader()
					lead.id = rf.me
					lead.term++
					lead.vote = vote
					rf.mu.Unlock()
					rf.heartBeat <- true
					fmt.Println(rf.me, "成为了主节点，开始发送心跳信号")
					return true
				}
			}
		}
	}
	return false
}

func (rf *raft) becomeCandidate() {
	rf.state = Candidate
	rf.setTerm(rf.currentTerm + 1)
	rf.voteFor = rf.me
	rf.currentLeader = NotLeader
}

func (rf *raft) becomeLeader() {
	rf.state = Leader
	rf.currentLeader = rf.me
}

func (rf *raft) sendLeaderHeartBeat() {
	for {
		select {
		case <-rf.heartBeat:
			rf.sendAppendEntriesImpl()
		}
	}
}

func (rf *raft) sendAppendEntriesImpl() {
	if rf.currentLeader == rf.me {
		successCount := 0
		for i := 0; i < RaftCount; i++ {
			if i != rf.me {
				go func() {
					rf.heartBeatReturn <- true
				}()
			}
		}

		for i := 0; i < RaftCount; i++ {
			select {
			case ok := <-rf.heartBeatReturn:
				if ok {
					successCount++
					if successCount > RaftCount/2 {
						fmt.Println("投票检查成功！心跳检查正常")
					}
				}
			}
		}
	}
}

func main() {
	for i := 0; i < RaftCount; i++ {
		initialize(i)
	}
	for {
		time.Sleep(time.Hour)
	}
}
