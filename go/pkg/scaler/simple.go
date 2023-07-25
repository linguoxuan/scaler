/*
Copyright 2023 The Alibaba Cloud Serverless Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scaler

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AliyunContainerService/scaler/go/pkg/config"
	model2 "github.com/AliyunContainerService/scaler/go/pkg/model"
	platform_client2 "github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

type Simple struct {
	config         *config.Config
	metaData       *model2.Meta
	platformClient platform_client2.Client
	mu             sync.Mutex
	wg             sync.WaitGroup
	instances      map[string]*model2.Instance
	waitingQueue   *list.List
	idleInstance   *list.List
}

/*
todo:外层使用memory打散，内层使用meta.key打散
instance有状态：创建/属于哪个meta.key/可复用/删除
内层gc将instance改为可复用的
外层gc真正把instance删除
*/
func New(metaData *model2.Meta, config *config.Config) Scaler {
	client, err := platform_client2.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Simple{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		mu:             sync.Mutex{},
		wg:             sync.WaitGroup{},
		instances:      make(map[string]*model2.Instance),
		waitingQueue:   list.New(),
		idleInstance:   list.New(),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		scheduler.gcLoop()
		log.Printf("gc loop for app: %s is stoped", metaData.Key)
	}()

	return scheduler
}

func (s *Simple) NeedCreate(ctx context.Context, requestId string) {
	//Create new Instance
	resourceConfig := model2.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: s.metaData.MemoryInMb,
		},
	}
	slot, err := s.platformClient.CreateSlot(ctx, requestId, &resourceConfig)
	if err != nil {
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		log.Println(errorMessage)
		return
	}

	meta := &model2.Meta{
		Meta: pb.Meta{
			Key:           s.metaData.Key,
			Runtime:       s.metaData.Runtime,
			TimeoutInSecs: s.metaData.TimeoutInSecs,
			MemoryInMb:    s.metaData.MemoryInMb,
		},
	}
	instanceId := uuid.New().String()
	instance, err := s.platformClient.Init(ctx, requestId, instanceId, slot, meta)
	if err != nil {
		errorMessage := fmt.Sprintf("create instance failed with: %s", err.Error())
		log.Println(errorMessage)
		return
	}
	//add new instance
	s.mu.Lock()
	if element := s.waitingQueue.Front(); element != nil {
		s.instances[instance.Id] = instance
		retCh := element.Value.(*chan string)
		s.waitingQueue.Remove(element)
		instance.Busy = true
		s.mu.Unlock()
		*retCh <- instance.Id
		close(*retCh)
	} else {
		curTaskCnt := len(s.instances) - s.idleInstance.Len() + s.waitingQueue.Len()
		curInstanceCnt := len(s.instances)
		if curInstanceCnt > s.config.MaxInstanceExisPerTask*curTaskCnt {
			s.mu.Unlock()
			reason := fmt.Sprintln("idle instance")
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
		} else {
			s.instances[instance.Id] = instance
			s.idleInstance.PushFront(instance)
			instance.Busy = false
			s.mu.Unlock()
		}

	}

}

func (s *Simple) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := ""
	defer func() {
		log.Printf("Assign, request id: %s, instance id: %s, cost %dms", request.RequestId, instanceId, time.Since(start).Milliseconds())
	}()
	log.Printf("Assign, request id: %s", request.RequestId)
	s.mu.Lock()
	if element := s.idleInstance.Front(); element != nil {
		instance := element.Value.(*model2.Instance)
		instance.Busy = true
		s.idleInstance.Remove(element)
		curInstanceCnt := len(s.instances)
		curTaskCnt := len(s.instances) - s.idleInstance.Len() + s.waitingQueue.Len()
		s.mu.Unlock()
		log.Printf("Assign, request id: %s, instance %s reused", request.RequestId, instance.Id)
		if curTaskCnt > curInstanceCnt*s.config.TaskPerInstance {
			go s.NeedCreate(ctx, request.RequestId)
		}
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    instance.Meta.Key,
				InstanceId: instance.Id,
			},
			ErrorMessage: nil,
		}, nil
	}
	retCh := make(chan string, 1)
	s.waitingQueue.PushBack(&retCh)
	curInstanceCnt := len(s.instances)
	curTaskCnt := len(s.instances) - s.idleInstance.Len() + s.waitingQueue.Len()
	s.mu.Unlock()
	if curTaskCnt > curInstanceCnt*s.config.TaskPerInstance {
		go s.NeedCreate(ctx, request.RequestId)
	}
	select {
	case instanceId = <-retCh:
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    request.MetaData.Key,
				InstanceId: instanceId,
			},
			ErrorMessage: nil,
		}, nil
	case <-time.After(10 * time.Second):
		return nil, status.Error(codes.Internal, fmt.Sprintln("timeout, may be create instance fail"))
	}
}

func (s *Simple) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintln("assignment is nil"))
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	defer func() {
		log.Printf("Idle, request id: %s, instance: %s, cost %dus", request.Assigment.RequestId, instanceId, time.Since(start).Microseconds())
	}()
	//log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	needDestroy := false
	slotId := ""
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}
	curTaskCnt := 0
	curInstanceCnt := 0
	defer func() {
		if needDestroy {
			if curTaskCnt > curInstanceCnt*s.config.TaskPerInstance {
				go s.NeedCreate(ctx, uuid.NewString())
			}
			s.deleteSlot(ctx, request.Assigment.RequestId, slotId, instanceId, request.Assigment.MetaKey, "bad instance")
		}
	}()
	log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	s.mu.Lock()
	defer s.mu.Unlock()
	curInstanceCnt = len(s.instances)
	curTaskCnt = len(s.instances) - s.idleInstance.Len() + s.waitingQueue.Len()
	if instance := s.instances[instanceId]; instance != nil {
		slotId = instance.Slot.Id
		instance.LastIdleTime = time.Now()
		if needDestroy {
			delete(s.instances, instance.Id)
			curInstanceCnt -= 1
			curTaskCnt -= 1
			log.Printf("request id %s, instance %s need be destroy", request.Assigment.RequestId, instanceId)
			return reply, nil
		}

		if !instance.Busy {
			log.Printf("request id %s, instance %s already freed", request.Assigment.RequestId, instanceId)
			return reply, nil
		}
		instance.Busy = false
		curTaskCnt -= 1

		if element := s.waitingQueue.Front(); element != nil {
			retCh := element.Value.(*chan string)
			s.waitingQueue.Remove(element)
			instance.Busy = true
			*retCh <- instance.Id
			close(*retCh)
		} else {
			s.idleInstance.PushFront(instance)
		}
	} else {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}
	return &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}, nil
}

func (s *Simple) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string) {
	log.Printf("start delete Instance %s (Slot: %s) of app: %s", instanceId, slotId, metaKey)
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		log.Printf("delete Instance %s (Slot: %s) of app: %s failed with: %s", instanceId, slotId, metaKey, err.Error())
	}
}

func (s *Simple) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		for {
			s.mu.Lock()
			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model2.Instance)
				idleDuration := time.Now().Sub(instance.LastIdleTime)
				if idleDuration > s.config.IdleDurationBeforeGC {
					//need GC
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					s.mu.Unlock()
					go func() {
						reason := fmt.Sprintf("Idle duration: %fs, excceed configured duration: %fs", idleDuration.Seconds(), s.config.IdleDurationBeforeGC.Seconds())
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()

					continue
				}
			}
			s.mu.Unlock()
			break
		}
	}
}

func (s *Simple) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
}
