package memcached

import (
	"sync"
)

type pipelineFrame struct {
	req       *request
	resp      *response
	start     int
	end       int
	tempDst   []GetResult
	temporary bool
}

type pipelineFrameScratch struct {
	frames []pipelineFrame
	keys   [][]byte
	dst    []GetResult
}

const _maxPooledPipelineFrames = 256

var _pipelineFramePool = sync.Pool{
	New: func() any {
		return &pipelineFrameScratch{
			frames: make([]pipelineFrame, 0, 16),
			keys:   make([][]byte, 0, 16),
			dst:    make([]GetResult, 0, 16),
		}
	},
}

// pipelineFrames combines keys from one Pipeline.Get call when they route to
// the same node and no intervening operation on that node separates them.
func pipelineFrames(ops []pipeOp) ([]pipelineFrame, *pipelineFrameScratch) {
	coalescedKeys := 0
	for i := 0; i < len(ops); {
		j := i + 1
		if ops[i].getGroup != 0 {
			for j < len(ops) && ops[j].getGroup == ops[i].getGroup {
				j++
			}
			if j-i > 1 {
				coalescedKeys += j - i
			}
		}
		i = j
	}
	if coalescedKeys == 0 {
		return nil, nil
	}

	scratch, ok := _pipelineFramePool.Get().(*pipelineFrameScratch)
	if !ok || scratch == nil {
		scratch = &pipelineFrameScratch{
			frames: make([]pipelineFrame, 0, 16),
			keys:   make([][]byte, 0, 16),
			dst:    make([]GetResult, 0, 16),
		}
	}
	if cap(scratch.keys) < coalescedKeys {
		scratch.keys = make([][]byte, coalescedKeys)
	}
	scratch.keys = scratch.keys[:coalescedKeys]
	if cap(scratch.dst) < coalescedKeys {
		scratch.dst = make([]GetResult, coalescedKeys)
	}
	scratch.dst = scratch.dst[:coalescedKeys]
	if cap(scratch.frames) < len(ops) {
		scratch.frames = make([]pipelineFrame, 0, len(ops))
	}
	scratch.frames = scratch.frames[:0]
	keys := scratch.keys
	dst := scratch.dst
	frames := scratch.frames
	tempAt := 0
	for i := 0; i < len(ops); {
		j := i + 1
		if ops[i].getGroup != 0 {
			for j < len(ops) && ops[j].getGroup == ops[i].getGroup {
				j++
			}
		}
		if j-i == 1 {
			frames = append(frames, pipelineFrame{
				req: ops[i].req, resp: ops[i].resp, start: i, end: j,
			})
			i = j
			continue
		}

		groupKeys := keys[tempAt : tempAt+j-i]
		groupDst := dst[tempAt : tempAt+j-i]
		for k := i; k < j; k++ {
			groupKeys[k-i] = ops[k].key
			groupDst[k-i].Value = ops[k].getDst.Value
		}
		var req *request
		// Extra was validated at enqueue; coalesced rebuild uses the same tokens.
		if ops[i].getCommand == "gat" || ops[i].getCommand == "gats" {
			req, _ = encodeGetAndTouchesCommand(ops[i].getCommand, ops[i].getExpiry, ops[i].getExtra, groupKeys...)
		} else {
			req, _ = encodeGetsCommand(ops[i].getCommand, ops[i].getExtra, groupKeys...)
		}
		frames = append(frames, pipelineFrame{
			req:       req,
			resp:      buildRetrievalResponse(groupKeys, nil, groupDst, ops[i].getWithCAS),
			start:     i,
			end:       j,
			tempDst:   groupDst,
			temporary: true,
		})
		tempAt += j - i
		i = j
	}
	scratch.frames = frames
	return frames, scratch
}

func releasePipelineFrames(scratch *pipelineFrameScratch) {
	if scratch == nil {
		return
	}
	for i := range scratch.frames {
		if scratch.frames[i].temporary {
			releaseReqAndResp(scratch.frames[i].req, scratch.frames[i].resp)
		}
		scratch.frames[i] = pipelineFrame{}
	}
	for i := range scratch.keys {
		scratch.keys[i] = nil
		scratch.dst[i] = GetResult{}
	}
	if cap(scratch.frames) > _maxPooledPipelineFrames ||
		cap(scratch.keys) > _maxPooledPipelineFrames ||
		cap(scratch.dst) > _maxPooledPipelineFrames {
		return
	}
	scratch.frames = scratch.frames[:0]
	scratch.keys = scratch.keys[:0]
	scratch.dst = scratch.dst[:0]
	_pipelineFramePool.Put(scratch)
}

func setPipelineResultError(ops []pipeOp, err error) {
	for i := range ops {
		if ops[i].result != nil {
			ops[i].result.Err = err
		}
	}
}

// runAddr writes then reads one node's queued frames on a single connection.
// When Cluster.MaxPipelineOps > 0, ops are split into waves so MaxOpen waiters
