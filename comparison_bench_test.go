package quaminapb_test

import (
	"os"
	"testing"

	quamina "quamina.net/go/quamina/v2"
	"google.golang.org/protobuf/reflect/protoreflect"

	quaminapb "github.com/spenczar/quaminapb"
	"github.com/spenczar/quaminapb/internal/testproto"
)

var (
	cityLotJSONHit  []byte
	cityLotJSONMiss []byte
	cityLotProtoHit []byte
	cityLotProtoMiss []byte

	statusJSONHit  []byte
	statusJSONMiss []byte
	statusProtoHit []byte
	statusProtoMiss []byte
)

var (
	cityLotDesc protoreflect.MessageDescriptor
	statusDesc  protoreflect.MessageDescriptor
)

func init() {
	cityLotDesc = (&testproto.CityLotFeature{}).ProtoReflect().Descriptor()
	statusDesc = (&testproto.StatusEvent{}).ProtoReflect().Descriptor()

	// CityLot JSON events (~600 bytes each)
	cityLotJSONHit = []byte(`{"type":"Feature","properties":{"MAPBLKLOT":"0001001","BLKLOT":"0001001","BLOCK_NUM":"0001","LOT_NUM":"001","FROM_ST":"","TO_ST":"","STREET":"CRANLEIGH","ST_TYPE":"DR","ODD_EVEN":"E"},"geometry":{"type":"Polygon","coordinates":[[[37.807,122.406,0.0],[37.808,122.407,0.0],[37.809,122.408,0.0],[37.807,122.406,0.0]]]}}`)
	cityLotJSONMiss = []byte(`{"type":"Feature","properties":{"MAPBLKLOT":"0001001","BLKLOT":"0001001","BLOCK_NUM":"0001","LOT_NUM":"001","FROM_ST":"","TO_ST":"","STREET":"GREENWICH","ST_TYPE":"DR","ODD_EVEN":"E"},"geometry":{"type":"Polygon","coordinates":[[[37.807,122.406,0.0],[37.808,122.407,0.0],[37.809,122.408,0.0],[37.807,122.406,0.0]]]}}`)

	// CityLot proto events
	cityLotProtoHit = mustMarshal(&testproto.CityLotFeature{
		Type: "Feature",
		Properties: &testproto.CityLotProperties{
			Mapblklot: "0001001",
			Blklot:    "0001001",
			BlockNum:  "0001",
			LotNum:    "001",
			FromSt:    "",
			ToSt:      "",
			Street:    "CRANLEIGH",
			StType:    "DR",
			OddEven:   "E",
		},
		Geometry: &testproto.CityLotGeometry{
			Type: "Polygon",
			Rings: []*testproto.CityLotRing{
				{Points: []*testproto.CityLotCoord{
					{Lon: 37.807, Lat: 122.406, Elev: 0.0},
					{Lon: 37.808, Lat: 122.407, Elev: 0.0},
					{Lon: 37.809, Lat: 122.408, Elev: 0.0},
					{Lon: 37.807, Lat: 122.406, Elev: 0.0},
				}},
			},
		},
	})
	cityLotProtoMiss = mustMarshal(&testproto.CityLotFeature{
		Type: "Feature",
		Properties: &testproto.CityLotProperties{
			Mapblklot: "0001001",
			Blklot:    "0001001",
			BlockNum:  "0001",
			LotNum:    "001",
			FromSt:    "",
			ToSt:      "",
			Street:    "GREENWICH",
			StType:    "DR",
			OddEven:   "E",
		},
		Geometry: &testproto.CityLotGeometry{
			Type: "Polygon",
			Rings: []*testproto.CityLotRing{
				{Points: []*testproto.CityLotCoord{
					{Lon: 37.807, Lat: 122.406, Elev: 0.0},
					{Lon: 37.808, Lat: 122.407, Elev: 0.0},
					{Lon: 37.809, Lat: 122.408, Elev: 0.0},
					{Lon: 37.807, Lat: 122.406, Elev: 0.0},
				}},
			},
		},
	})

	// Status JSON events — hit event read from testdata/status.json (9.4 KB).
	// The miss event is a minimal JSON with the same field paths but non-matching values.
	var err error
	statusJSONHit, err = os.ReadFile("testdata/status.json")
	if err != nil {
		panic("init: read testdata/status.json: " + err.Error())
	}
	statusJSONMiss = []byte(`{"context":{"user_id":9999,"friends_count":0},"payload":{"user":{"id_str":"000000"},"lang_value":"en"}}`)

	// Status proto events
	statusProtoHit = mustMarshal(&testproto.StatusEvent{
		Context: &testproto.StatusContext{
			UserId:       9034,
			FriendsCount: 158,
		},
		Payload: &testproto.StatusPayload{
			Metadata: &testproto.StatusPayloadMetadata{
				ResultType:      "recent",
				IsoLanguageCode: "ja",
			},
			CreatedAt:     "Sun Aug 31 00:29:14 +0000 2014",
			Id:            505874922023837696,
			IdStr:         "505874922023837696",
			Text:          "RT @KATANA77: えっそれは・・・（一同） http://t.co/PkCJAcSuYK",
			Source:        `<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>`,
			Truncated:     false,
			User: &testproto.StatusUser{
				Id:             903487807,
				IdStr:          "903487807",
				Name:           "RT&ファボ魔のむっつんさっm",
				ScreenName:     "yuttari1998",
				Location:       "関西    ↓詳しいプロ↓",
				Description:    "無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます",
				FollowersCount: 95,
				FriendsCount:   158,
				ListedCount:    1,
				StatusesCount:  10276,
				CreatedAt:      "Thu Oct 25 08:27:13 +0000 2012",
				Verified:       false,
			},
			RetweetCount:  82,
			FavoriteCount: 0,
			Favorited:     false,
			Retweeted:     false,
			LangValue:     "ja",
		},
	})
	statusProtoMiss = mustMarshal(&testproto.StatusEvent{
		Context: &testproto.StatusContext{
			UserId:       9999,
			FriendsCount: 0,
		},
		Payload: &testproto.StatusPayload{
			User: &testproto.StatusUser{
				IdStr: "000000",
			},
			LangValue: "en",
		},
	})

}

// newJSONQuamina constructs a quamina.Quamina instance using the default JSON
// flattener with the given patterns pre-loaded.
func newJSONQuamina(b *testing.B, patterns map[string]string) *quamina.Quamina {
	b.Helper()
	q, err := quamina.New()
	if err != nil {
		b.Fatal(err)
	}
	for name, pat := range patterns {
		if err := q.AddPattern(quamina.X(name), pat); err != nil {
			b.Fatalf("AddPattern(%q): %v", name, err)
		}
	}
	return q
}

// newProtoQuamina constructs a quamina.Quamina instance using the proto
// flattener for the given message descriptor, with the given patterns pre-loaded.
func newProtoQuamina(b *testing.B, desc protoreflect.MessageDescriptor, patterns map[string]string) *quamina.Quamina {
	b.Helper()
	fl := quaminapb.New(desc)
	q, err := quamina.New(quamina.WithFlattener(fl))
	if err != nil {
		b.Fatal(err)
	}
	for name, pat := range patterns {
		if err := q.AddPattern(quamina.X(name), pat); err != nil {
			b.Fatalf("AddPattern(%q): %v", name, err)
		}
	}
	return q
}

// cityLotJSONPatterns matches on the STREET field using JSON (uppercase field names
// as they appear in citylots GeoJSON features).
var cityLotJSONPatterns = map[string]string{
	"street-match": `{"properties": {"STREET": ["CRANLEIGH"]}}`,
}

// cityLotProtoPatterns matches on the street field using proto (lowercase field names
// as defined in the CityLotProperties message). Note: proto field names are lowercase
// where JSON uses uppercase; both exercise the same nested repeated-message traversal.
var cityLotProtoPatterns = map[string]string{
	"street-match": `{"properties": {"street": ["CRANLEIGH"]}}`,
}

// statusPatterns are identical for JSON and proto: field names in status.json
// are already lowercase and match the proto field names.
var statusPatterns = map[string]string{
	"context-match": `{"context": {"user_id": [9034], "friends_count": [158]}}`,
	"user-match":    `{"payload": {"user": {"id_str": ["903487807"]}}}`,
	"lang-match":    `{"payload": {"lang_value": ["ja"]}}`,
}

func Benchmark_CityLot_JSON_Hit(b *testing.B) {
	q := newJSONQuamina(b, cityLotJSONPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(cityLotJSONHit)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

func Benchmark_CityLot_JSON_Miss(b *testing.B) {
	q := newJSONQuamina(b, cityLotJSONPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(cityLotJSONMiss)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

func Benchmark_CityLot_Proto_Hit(b *testing.B) {
	q := newProtoQuamina(b, cityLotDesc, cityLotProtoPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(cityLotProtoHit)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

func Benchmark_CityLot_Proto_Miss(b *testing.B) {
	q := newProtoQuamina(b, cityLotDesc, cityLotProtoPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(cityLotProtoMiss)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

func Benchmark_Status_JSON_Hit(b *testing.B) {
	q := newJSONQuamina(b, statusPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(statusJSONHit)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

func Benchmark_Status_JSON_Miss(b *testing.B) {
	q := newJSONQuamina(b, statusPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(statusJSONMiss)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

func Benchmark_Status_Proto_Hit(b *testing.B) {
	q := newProtoQuamina(b, statusDesc, statusPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(statusProtoHit)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

func Benchmark_Status_Proto_Miss(b *testing.B) {
	q := newProtoQuamina(b, statusDesc, statusPatterns)
	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(statusProtoMiss)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}
