package batch_working_queue

import (
	"testing"
)

type itemTest struct {
	key string
}

func TestBatchWorkingQueue(t *testing.T) {
	type step struct {
		add     *itemTest
		get     func(t *testing.T, workKey string, work []*itemTest)
		doneKey string
	}

	defaultKeyFunc := func(obj interface{}) string {
		return obj.(*itemTest).key
	}

	scenarios := []struct {
		name  string
		steps []*step
	}{
		{name: "get and done on empty queue works", steps: []*step{{get: validateGet("", 0), doneKey: "1"}}},
		{
			name: "single item",
			steps: []*step{
				/* q: 1     */ {add: &itemTest{"1"}}, {add: &itemTest{"1"}},
				/* q: empty */ {get: validateGet("1", 2)},
				/* q: 1     */ {add: &itemTest{"1"}, doneKey: "1"},
				/* q: empty */ {get: validateGet("1", 1)},
			},
		},
		{
			name: "multiple items - key 1 re-queued",
			steps: []*step{
				/* q: 1, 2  */ {add: &itemTest{"1"}}, {add: &itemTest{"1"}}, {add: &itemTest{"2"}},
				/* q: 2     */ {get: validateGet("1", 2)},
				/* q: 2, 1  */ {add: &itemTest{"1"}}, {add: &itemTest{"2"}, doneKey: "1"},
				/* q: 1     */ {get: validateGet("2", 2)},
				/* q: empty */ {get: validateGet("1", 1)},
			},
		},
		{
			name: "multiple items - multiple items re-queued",
			steps: []*step{
				/* q: 1, 2    */ {add: &itemTest{"1"}}, {add: &itemTest{"1"}}, {add: &itemTest{"2"}},
				/* q: 2       */ {get: validateGet("1", 2)},
				/* q: 2, 3, 4 */ {add: &itemTest{"3"}}, {add: &itemTest{"4"}}, {add: &itemTest{"3"}},
				/* q: 3, 4    */ {get: validateGet("2", 1)},
				/* q: 3, 4    */ {doneKey: "1"}, {add: &itemTest{"2"}},
				/* q: 3, 4, 2 */ {doneKey: "2"}, {add: &itemTest{"2"}},
				/* q: 4, 2    */ {get: validateGet("3", 2)},
				/* q: 2       */ {get: validateGet("4", 1)},
				/* q: empty   */ {get: validateGet("2", 2)},
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// test data

			target := New(defaultKeyFunc)
			for _, step := range scenario.steps {
				if step.add != nil {
					target.Add(step.add)
				}
				if step.get != nil {
					workKey, work := target.Get()
					step.get(t, workKey, convert(work))
				}
				target.Done(step.doneKey)
			}
		})
	}
}

func convert(raw []interface{}) []*itemTest {
	ret := make([]*itemTest, len(raw))
	for i, r := range raw {
		ret[i] = r.(*itemTest)
	}
	return ret
}

func validateGet(expectedWorkKey string, expectedWorkLen int) func(t *testing.T, workKey string, work []*itemTest) {
	return func(t *testing.T, workKey string, work []*itemTest) {
		if workKey != expectedWorkKey {
			t.Fatalf("unexpected key returned %q, expected %q", workKey, expectedWorkKey)
		}
		if len(work) != expectedWorkLen {
			t.Fatalf("expected to get %d items, got %d", expectedWorkLen, len(work))
		}
		// TODO: validate content
	}
}
