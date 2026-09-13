package memcached

import (
	"testing"
)

func Test_parseStats(t *testing.T) {
	type args struct {
		lines [][]byte
	}
	tests := []struct {
		name    string
		args    args
		want    *Statistic
		wantErr bool
	}{
		{
			name: "normal case",
			args: args{
				lines: [][]byte{
					[]byte("STAT version 1.5.12"),
					[]byte("STAT pid 12345"),
					[]byte("STAT uptime 123456"),
					[]byte("STAT time 1234567890"),
					[]byte("STAT pointer_size 64"),
					[]byte("STAT rusage_user 30440.595477"),
					[]byte("STAT rusage_system 41317.488860"),
					[]byte("STAT curr_connections 123"),
					[]byte("STAT total_connections 123456"),
					[]byte("STAT connection_structures 1234567890"),
					[]byte("STAT reserved_fds 1234567890"),
					[]byte("STAT accepting_conns 1"),
					[]byte("STAT hash_is_expanding 1"),
					[]byte("STAT custom_ext_key 99"),
				},
			},
			want: &Statistic{
				Version:              "1.5.12",
				PID:                  12345,
				Uptime:               123456,
				Time:                 1234567890,
				PointerSize:          64,
				RusageUser:           30440.595477,
				RusageSystem:         41317.488860,
				CurrConnections:      123,
				TotalConnections:     123456,
				ConnectionStructures: 1234567890,
				ReservedFDs:          1234567890,
				AcceptingConns:       true,
				HashIsExpanding:      true,
				Extra:                map[string]string{"custom_ext_key": "99"},
			},
		},
		{
			name: "END only",
			args: args{
				lines: [][]byte{[]byte("END\r\n")},
			},
			want: &Statistic{},
		},
		{
			name: "unparseable known key goes to Extra",
			args: args{
				lines: [][]byte{
					[]byte("STAT pid not-a-number"),
					[]byte("STAT version 1.6.0 with build info"),
				},
			},
			want: &Statistic{
				Version: "1.6.0 with build info",
				Extra:   map[string]string{"pid": "not-a-number"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStats(tt.args.lines)
			if tt.wantErr {
				isError(t, err)
				return
			}
			noError(t, err)
			equal(t, tt.want, got)
		})
	}
}

func Test_parseStatsMap(t *testing.T) {
	got, err := parseStatsMap([][]byte{
		[]byte("STAT items:1:number 2\r\n"),
		[]byte("END\r\n"),
	})
	noError(t, err)
	equal(t, "2", got["items:1:number"])
}
