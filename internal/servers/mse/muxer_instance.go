package mse

import (
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4/seekablebuffer"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/websocket"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/unit"
)

type mseTrack struct {
	init *fmp4.InitTrack
	// state for parting
	firstDTS   int64
	lastDTS    int64
	samples    []*fmp4.Sample
	baseOffset int64
}

type muxerInstance struct {
	pathName string
	stream   *stream.Stream
	parent   instanceParent

	bcast *wsBroadcaster

	mu        sync.Mutex
	inited    bool
	initBuf   []byte
	tracks    []*mseTrack
	nextSeq   uint32
	partStart time.Duration
	partBuf   seekablebuffer.Buffer

	mimeType string
}

func (mi *muxerInstance) initialize() error {
	mi.bcast = newWSBroadcaster()

	// setup tracks (support H264 and MPEG4Audio for now)
	var h264fmt *format.H264
	var h264med *description.Media
	if h264med = mi.stream.Desc.FindFormat(&h264fmt); h264fmt != nil {
		sps, pps := h264fmt.SafeParams()
		if sps != nil && pps != nil {
			mi.addH264Track(h264med, h264fmt, sps, pps)
		}
	}
	var aacfmt *format.MPEG4Audio
	var aacmed *description.Media
	if aacmed = mi.stream.Desc.FindFormat(&aacfmt); aacfmt != nil {
		mi.addAACTrack(aacmed, aacfmt)
	}

	if len(mi.tracks) == 0 {
		return nil
	}

	// choose a conservative MIME type based on available tracks
	hasVideo := false
	hasAudio := false
	for _, t := range mi.tracks {
		switch t.init.Codec.(type) {
		case *fmp4.CodecH264:
			hasVideo = true
		case *fmp4.CodecMPEG4Audio:
			hasAudio = true
		}
	}
	switch {
	case hasVideo && hasAudio:
		mi.mimeType = `video/mp4; codecs="avc1.42E01E, mp4a.40.2"`
	case hasVideo:
		mi.mimeType = `video/mp4; codecs="avc1.42E01E"`
	case hasAudio:
		mi.mimeType = `audio/mp4; codecs="mp4a.40.2"`
	}

	// build and send init
	init := fmp4.Init{Tracks: make([]*fmp4.InitTrack, len(mi.tracks))}
	for i, t := range mi.tracks {
		init.Tracks[i] = t.init
	}
	var ib seekablebuffer.Buffer
	if err := init.Marshal(&ib); err == nil {
		mi.initBuf = append([]byte(nil), ib.Bytes()...)
		mi.inited = true
		mi.bcast.Broadcast(mi.initBuf)
	}

	// start stream reading
	mi.stream.StartReader(mi)
	return nil
}

func (mi *muxerInstance) close() {
	mi.stream.RemoveReader(mi)
}

func (mi *muxerInstance) Log(level logger.Level, format string, args ...interface{}) {
	mi.parent.Log(level, format, args...)
}

func (mi *muxerInstance) handleWS(c *websocket.ServerConn) {
	// send MIME type hint first so the client can allocate the proper SourceBuffer
	if mi.mimeType != "" {
		_ = c.WriteJSON(struct {
			Mime string `json:"mime"`
		}{Mime: mi.mimeType})
	}
	if mi.inited && mi.initBuf != nil {
		_ = c.Write(mi.initBuf)
	}
	ch := mi.bcast.Subscribe()
	defer mi.bcast.Unsubscribe(ch)
	for byts := range ch {
		// mark activity to keep muxer alive while client is consuming
		mi.parent.touchActivity()
		if err := c.Write(byts); err != nil {
			return
		}
	}
}

func (mi *muxerInstance) addH264Track(med *description.Media, forma *format.H264, sps []byte, pps []byte) {
	trk := &mseTrack{init: &fmp4.InitTrack{ID: len(mi.tracks) + 1, TimeScale: uint32(forma.ClockRate()), Codec: &fmp4.CodecH264{SPS: sps, PPS: pps}}, firstDTS: -1}
	mi.tracks = append(mi.tracks, trk)
	var dtsExtractor h264.DTSExtractor
	dtsExtractor.Initialize()
	mi.stream.AddReader(mi, med, forma, func(u unit.Unit) error {
		tu := u.(*unit.H264)
		if tu.AU == nil {
			return nil
		}
		randomAccess := false
		for _, nalu := range tu.AU {
			if h264.NALUType(nalu[0]&0x1F) == h264.NALUTypeIDR {
				randomAccess = true
			}
		}
		dts, err := dtsExtractor.Extract(tu.AU, tu.PTS)
		if err != nil {
			return nil
		}
		if randomAccess && len(trk.samples) > 0 {
			// flush current parts when a new keyframe appears
			mi.flushParts(false)
		}
		mi.writeSample(trk, dts, int32(tu.PTS-dts), !randomAccess, tu.AU)
		return nil
	})
}

func (mi *muxerInstance) addAACTrack(med *description.Media, forma *format.MPEG4Audio) {
	trk := &mseTrack{init: &fmp4.InitTrack{ID: len(mi.tracks) + 1, TimeScale: uint32(forma.ClockRate()), Codec: &fmp4.CodecMPEG4Audio{Config: *forma.Config}}, firstDTS: -1}
	mi.tracks = append(mi.tracks, trk)
	mi.stream.AddReader(mi, med, forma, func(u unit.Unit) error {
		tu := u.(*unit.MPEG4Audio)
		if tu.AUs == nil {
			return nil
		}
		for i, au := range tu.AUs {
			pts := tu.PTS + int64(i)*mpeg4audio.SamplesPerAccessUnit
			mi.writeSample(trk, pts, 0, false, [][]byte{au})
		}
		return nil
	})
}

func (mi *muxerInstance) writeSample(trk *mseTrack, dts int64, ptsOffset int32, nonSync bool, payload [][]byte) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	if dts >= 0 {
		if trk.firstDTS < 0 {
			trk.firstDTS = dts
			trk.samples = trk.samples[:0]
		} else {
			dur := dts - trk.lastDTS
			if dur < 0 {
				dur = 0
			}
			trk.samples[len(trk.samples)-1].Duration = uint32(dur)
		}
	}
	var samp fmp4.Sample
	switch trk.init.Codec.(type) {
	case *fmp4.CodecH264:
		_ = samp.FillH264(ptsOffset, payload)
	case *fmp4.CodecMPEG4Audio:
		samp.Payload = payload[0]
	}
	samp.IsNonSyncSample = nonSync
	trk.samples = append(trk.samples, &samp)
	trk.lastDTS = dts

	mi.maybeFlushParts()
}

func (mi *muxerInstance) maybeFlushParts() { mi.flushParts(false) }

func (mi *muxerInstance) flushParts(force bool) {
	// build part only when we have at least 2 samples per track
	// so the last sample in a part always has a valid duration.
	var part fmp4.Part
	ready := false
	for _, trk := range mi.tracks {
		if trk.firstDTS < 0 || (!force && len(trk.samples) < 2) || (force && len(trk.samples) == 0) {
			continue
		}
		// normalize timeline so first appended timestamp is 0
		if trk.baseOffset == 0 {
			trk.baseOffset = trk.firstDTS
		}
		base := trk.firstDTS - trk.baseOffset
		if base < 0 {
			base = 0
		}
		pt := &fmp4.PartTrack{ID: trk.init.ID, BaseTime: uint64(base)}
		// exclude the last sample (duration not known yet)
		if !force || len(trk.samples) > 1 {
			pt.Samples = append(pt.Samples, trk.samples[:len(trk.samples)-1]...)
		}
		if len(pt.Samples) == 0 {
			continue
		}
		part.Tracks = append(part.Tracks, pt)
		ready = true
	}
	if !ready {
		return
	}
	part.SequenceNumber = mi.nextSeq
	mi.nextSeq++
	mi.partBuf.Reset()
	if err := part.Marshal(&mi.partBuf); err == nil {
		mi.bcast.Broadcast(mi.partBuf.Bytes())
	}
	// keep last sample for duration correction
	for _, trk := range mi.tracks {
		if (!force && len(trk.samples) >= 2) || (force && len(trk.samples) >= 1) {
			trk.samples = trk.samples[len(trk.samples)-1:]
			trk.firstDTS = trk.lastDTS
		}
	}
}

// wsBroadcaster broadcasts byte slices to subscribers.
type wsBroadcaster struct {
	mu   sync.RWMutex
	subs map[chan []byte]struct{}
}

func newWSBroadcaster() *wsBroadcaster { return &wsBroadcaster{subs: make(map[chan []byte]struct{})} }

func (b *wsBroadcaster) Subscribe() chan []byte {
	ch := make(chan []byte, 32)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *wsBroadcaster) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.subs, ch)
	close(ch)
	b.mu.Unlock()
}

func (b *wsBroadcaster) Broadcast(byts []byte) {
	b.mu.RLock()
	for ch := range b.subs {
		select {
		case ch <- append([]byte(nil), byts...):
		default:
		}
	}
	b.mu.RUnlock()
}
