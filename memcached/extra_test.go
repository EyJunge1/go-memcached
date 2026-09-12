package memcached

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestValidMetaExtras(t *testing.T) {
	noError(t, validMetaExtras(nil))
	noError(t, validMetaExtras([]string{"Z1", "Y"}))
	isErrorIs(t, validMetaExtras([]string{""}), ErrInvalidArgument)
	isErrorIs(t, validMetaExtras([]string{"bad tok"}), ErrInvalidArgument)
	isErrorIs(t, validMetaExtras([]string{"q"}), ErrInvalidArgument)
	isErrorIs(t, validMetaExtras([]string{"b1"}), ErrInvalidArgument)
	isErrorIs(t, validMetaExtras([]string{"Otok"}), ErrInvalidArgument)
}

func TestValidTextExtras(t *testing.T) {
	noError(t, validTextExtras(nil))
	noError(t, validTextExtras([]string{"foo", "bar"}))
	isErrorIs(t, validTextExtras([]string{""}), ErrInvalidArgument)
	isErrorIs(t, validTextExtras([]string{"a b"}), ErrInvalidArgument)
	isErrorIs(t, validTextExtras([]string{"noreply"}), ErrInvalidArgument)
	isErrorIs(t, validTextExtras([]string{"NoReply"}), ErrInvalidArgument)
}

func TestMetaGetExtraEncoding(t *testing.T) {
	f := MetaGetValue().WithCAS().WithExtra("Z1", "Y")
	noError(t, f.validate())
	flags := f.toFlags()
	req, _ := buildMetaGetCommand([]byte("k"), &flags)
	defer releaseReqAndResp(req, nil)
	if !bytes.HasSuffix(req.raw, []byte("c v Z1 Y\r\n")) && !bytes.Contains(req.raw, []byte(" c v Z1 Y\r\n")) {
		// Flag order is fixed in writeMetaGet: c before v, extras last.
		idxC := bytes.Index(req.raw, []byte("c "))
		idxV := bytes.Index(req.raw, []byte("v "))
		idxZ := bytes.Index(req.raw, []byte("Z1"))
		idxY := bytes.Index(req.raw, []byte("Y"))
		if idxC < 0 || idxV < 0 || idxZ < 0 || idxY < 0 || idxC >= idxV || idxV >= idxZ || idxZ >= idxY {
			t.Fatalf("unexpected wire %q", req.raw)
		}
	}
}

func TestMetaGetExtraRejectsFraming(t *testing.T) {
	err := MetaGet{}.WithExtra("q").validate()
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v", err)
	}
}

func TestTextStoreExtraBeforeNoreply(t *testing.T) {
	req, resp, err := buildStorageCommand(_SetBytes, []byte("k"), []byte("v"), 0, ExptimeNever, []string{"foo"}, true)
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	want := []byte("set k 0 0 1 foo noreply\r\n")
	if !bytes.Equal(req.raw, want) {
		t.Fatalf("got %q want %q", req.raw, want)
	}
}

func TestTextGetExtraAfterKeys(t *testing.T) {
	req, err := encodeGetsCommand("get", []string{"foo"}, []byte("a"), []byte("b"))
	req = mustBuildReq(t, req, err)
	defer releaseReqAndResp(req, nil)
	want := []byte("get a b foo\r\n")
	if !bytes.Equal(req.raw, want) {
		t.Fatalf("got %q want %q", req.raw, want)
	}
}

func TestTextDeleteExtraBeforeNoreply(t *testing.T) {
	req, resp, err := buildDeleteCommand([]byte("k"), []string{"x"}, true)
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	want := []byte("delete k x noreply\r\n")
	if !bytes.Equal(req.raw, want) {
		t.Fatalf("got %q want %q", req.raw, want)
	}
}

func TestTextIncrExtra(t *testing.T) {
	req, resp, err := buildArithmeticCommand("incr", []byte("n"), 2, []string{"foo"}, false)
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	want := []byte("incr n 2 foo\r\n")
	if !bytes.Equal(req.raw, want) {
		t.Fatalf("got %q want %q", req.raw, want)
	}
}

func TestStoreRejectsNoreplyExtra(t *testing.T) {
	c := &Client{}
	err := c.Store(context.TODO(), Item{Key: []byte("k"), Value: []byte("v"), Extra: []string{"noreply"}})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v", err)
	}
}

func TestPipelineDeleteTouchIncrExtraEncoding(t *testing.T) {
	p := (&Client{}).Pipeline()
	var n uint64
	p.Delete([]byte("k"), MutateOpts{Extra: []string{"x"}}).
		Touch([]byte("k"), Rel(0), MutateOpts{Extra: []string{"y"}}).
		Incr([]byte("n"), 1, &n, ArithmeticOpts{Extra: []string{"z"}})
	if len(p.ops) != 3 {
		t.Fatalf("ops=%d", len(p.ops))
	}
	if !bytes.Equal(p.ops[0].req.raw, []byte("delete k x\r\n")) {
		t.Fatalf("delete wire %q", p.ops[0].req.raw)
	}
	if !bytes.Equal(p.ops[1].req.raw, []byte("touch k 0 y\r\n")) {
		t.Fatalf("touch wire %q", p.ops[1].req.raw)
	}
	if !bytes.Equal(p.ops[2].req.raw, []byte("incr n 1 z\r\n")) {
		t.Fatalf("incr wire %q", p.ops[2].req.raw)
	}
}

func TestPipelineRejectsQuietMutate(t *testing.T) {
	p := (&Client{}).Pipeline().Delete([]byte("k"), MutateOpts{Quiet: true})
	if len(p.errs) == 0 || !errors.Is(p.errs[0], ErrInvalidArgument) {
		t.Fatalf("errs=%v", p.errs)
	}
}
