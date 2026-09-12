package benchmark

import (
	"context"
	"testing"
)

func TestTCPSetGetSmoke(t *testing.T) {
	requireTCP(t)
	tests := tcpFactories()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg := tt.open(t)
			ctx := context.Background()
			key := "b/" + tt.name + "/smoke"
			if err := sg.set(ctx, key, _smallValue); err != nil {
				t.Fatal(err)
			}
			if err := sg.get(ctx, key); err != nil {
				t.Fatal(err)
			}
		})
	}
}
