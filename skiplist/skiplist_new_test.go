package skiplist

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"math/rand/v2"
)

// Helper functions for generating tests

func isSubset(subList, superList []int) bool {
	elemCount := make(map[int]int)
	for _, num := range superList {
		elemCount[num]++
	}

	// Check each element in the sub list
	for _, num := range subList {
		if elemCount[num] == 0 {
			return false // If any element in sub is not in super, return false
		}
		elemCount[num]-- // Decrement count to account for multiple occurrences
	}

	return true
}

// Gets the unique elements from a slice
func uniqueElements(slice []string) []string {
	uniqueMap := make(map[string]bool)
	uniqueSlice := make([]string, 0)

	for _, elem := range slice {
		// If the element is not in the map, add it to the uniqueSlice
		if _, exists := uniqueMap[elem]; !exists {
			uniqueMap[elem] = true
			uniqueSlice = append(uniqueSlice, elem)
		}
	}
	return uniqueSlice
}

// Generates a random string of length n
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, n)

	// Initializing random number generator with current time as seed
	src := rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))
	r := rand.New(src)

	for i := range result {
		result[i] = letters[r.IntN(len(letters))]
	}
	return string(result)
}

func makeRandomUniqueStrings(count int, length int) []string {
	string_slice := make([]string, 0)

	strings_added := make(map[string]bool, 0)

	for i := 0; i < count; i++ {

		rand_str := randomString(length)
		_, ok := strings_added[rand_str]
		for ok {
			rand_str = randomString(length)
			_, ok = strings_added[rand_str]
		}
		strings_added[rand_str] = true

		string_slice = append(string_slice, rand_str)
	}

	return string_slice
}

func makeRandomStrings(count int, length int) []string {
	string_slice := make([]string, 0)

	for i := 0; i < count; i++ {

		rand_str := randomString(length)
		string_slice = append(string_slice, rand_str)
	}

	return string_slice
}

func makeRandomInts(count int, max int) []int {
	int_slice := make([]int, 0)
	// Initializing random number generator with current time as seed
	src := rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))
	r := rand.New(src)
	for i := 0; i < count; i++ {
		int_slice = append(int_slice, r.IntN(max))
	}

	return int_slice
}

// Simple interfaces for updatecheck

func NewNoOverwriteCheck(new_val *int) UpdateCheck[string, int] {
	new_nooverwritecheck := func(key string, currVal *int, exists bool) (*int, error) {
		if exists {
			return nil, fmt.Errorf("Can't overwrite value")
		} else {
			return new_val, nil
		}
	}
	return new_nooverwritecheck
}

func NewOverwriteCheck(new_val *int) UpdateCheck[string, int] {
	new_nooverwritecheck := func(key string, currVal *int, exists bool) (*int, error) {
		if exists {
			*currVal = *new_val
			return nil, nil
		} else {
			return new_val, nil
		}
	}
	return new_nooverwritecheck
}

func DeepCopy(val *int) (*int, error) {
	new_val := *val
	return &new_val, nil
}

// Sequential tests of skiplist

func Test_EntryNotFound(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")
	_, found := skiplist.Find("hello")
	if found == true {
		t.Error("String not in skiplist was found")
	}
}

func Test_EmptyStringFind(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")
	_, found := skiplist.Find("")
	if found == true {
		t.Error("String not in skiplist was found")
	}
}

func Test_SimpleUpsertNoOverwrite(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num2 := 20
	skiplist.Upsert("ge", NewNoOverwriteCheck(&num2))

	elem_found, found := skiplist.Find("ge")

	if *elem_found != 20 || found != true {
		t.Error("Element not inserted properly")
	}
}

func Test_DuplicateNoOverwrite(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num2 := 20
	skiplist.Upsert("ge", NewNoOverwriteCheck(&num2))

	num3 := 12
	_, err := skiplist.Upsert("ge", NewNoOverwriteCheck(&num3))

	if err == nil {
		t.Error("Should not allow overwrites")
	}
}

func Test_SimpleUpsertOverwrite(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num2 := 20
	skiplist.Upsert("ge", NewOverwriteCheck(&num2))

	elem_found, found := skiplist.Find("ge")

	if *elem_found != 20 || found != true {
		t.Error("Element not inserted properly")
	}
}

func Test_DuplicateUpsertOverwrite(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num2 := 20
	skiplist.Upsert("ge", NewOverwriteCheck(&num2))

	num3 := 12
	skiplist.Upsert("ge", NewOverwriteCheck(&num3))

	found_val, _ := skiplist.Find("ge")

	if *found_val != 12 {
		t.Error("Element not overwritten correctly")
	}

}

func Test_MultipleKeysOverwrite(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num2 := 20
	skiplist.Upsert("ge", NewOverwriteCheck(&num2))

	num3 := 12
	skiplist.Upsert("he", NewOverwriteCheck(&num3))

	found_val_he, _ := skiplist.Find("he")
	found_val_ge, _ := skiplist.Find("ge")

	if *found_val_ge != 20 || *found_val_he != 12 {
		t.Error("Elements not inserted correctly")
	}
}

func Test_SimpleUpsertRemove(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num2 := 20
	skiplist.Upsert("ge", NewNoOverwriteCheck(&num2))

	skiplist.Delete("ge")

	_, found := skiplist.Find("ge")

	if found {
		t.Error("Element not properly removed")
	}
}

func Test_QueryMultipleKeys(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num2 := 20
	skiplist.Upsert("ge", NewOverwriteCheck(&num2))

	num3 := 12
	skiplist.Upsert("he", NewOverwriteCheck(&num3))

	skiplist.Delete("he")

	found_vals, _ := skiplist.Query("", "\U0010FFFF")

	for _, num := range found_vals {
		fmt.Println(*num)
	}
}

// Concurrent Tests

func Test_ConcurrentAddingElements(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 1000
	rand_strs := makeRandomUniqueStrings(num_elems, 4)
	rand_ints := makeRandomInts(num_elems, 100)

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg *sync.WaitGroup) {
			skiplist.Upsert(rand_strs[i], NewNoOverwriteCheck(&rand_ints[i]))
			wg.Done()
		}(&wg)
	}

	wg.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	new_int_list := make([]int, 0)
	for _, elem := range elems {
		new_int_list = append(new_int_list, *elem)
	}

	sort.Ints(new_int_list)
	sort.Ints(rand_ints)
	if !reflect.DeepEqual(new_int_list, rand_ints) {
		t.Error("All elements not added/added incorrectly")
	}
}

func Test_ConcurrentRemovingElements(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 1000
	rand_strs := makeRandomUniqueStrings(num_elems, 4)
	rand_ints := makeRandomInts(num_elems, 100)

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func() {
			skiplist.Upsert(rand_strs[i], NewNoOverwriteCheck(&rand_ints[i]))
			wg.Done()
		}()
	}

	wg.Wait()

	var wg2 sync.WaitGroup

	wg2.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg2 *sync.WaitGroup) {
			skiplist.Delete(rand_strs[i])
			wg2.Done()
		}(&wg2)
	}

	wg2.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	new_int_list := make([]int, 0)
	for _, elem := range elems {
		new_int_list = append(new_int_list, *elem)
	}

	if len(new_int_list) != 0 {
		t.Error("Should have removed all elements")
	}
}

func Test_ConcurrentRemovingInvalidElements(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 1000
	rand_strs := makeRandomUniqueStrings(num_elems, 4)
	rand_ints := makeRandomInts(num_elems, 100)

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func() {
			skiplist.Upsert(rand_strs[i], NewNoOverwriteCheck(&rand_ints[i]))
			wg.Done()
		}()
	}

	wg.Wait()

	var wg2 sync.WaitGroup

	wg2.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg2 *sync.WaitGroup) {
			if i > num_elems/2 {
				skiplist.Delete(rand_strs[i] + "random")
			} else {
				skiplist.Delete(rand_strs[i])
			}
			wg2.Done()
		}(&wg2)
	}

	wg2.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	new_int_list := make([]int, 0)
	for _, elem := range elems {
		new_int_list = append(new_int_list, *elem)
	}

	fmt.Println(len(new_int_list))
	if len(new_int_list) != num_elems/2-1 {
		t.Error("Should have removed all elements")
	}
}

func Test_ConcurrentAddingElementsWithDupes(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 1000
	rand_strs := makeRandomStrings(num_elems, 1)
	rand_ints := makeRandomInts(num_elems, 100)

	num_unique_strs := len(uniqueElements(rand_strs))

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg *sync.WaitGroup) {
			skiplist.Upsert(rand_strs[i], NewOverwriteCheck(&rand_ints[i]))
			wg.Done()
		}(&wg)
	}

	wg.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	if len(elems) != num_unique_strs {
		t.Error("All elements not added/added incorrectly")
	}
}

func Test_ConcurrentAddingElementsWithDupesNoOverwrite(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 1000
	rand_strs := makeRandomStrings(num_elems, 1)
	rand_ints := makeRandomInts(num_elems, 100)

	num_unique_strs := len(uniqueElements(rand_strs))

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg *sync.WaitGroup) {
			skiplist.Upsert(rand_strs[i], NewNoOverwriteCheck(&rand_ints[i]))
			wg.Done()
		}(&wg)
	}

	wg.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	if len(elems) != num_unique_strs {
		t.Error("All elements not added/added incorrectly")
	}

	skiplist.Visualize()
}

func Test_ConcurrentAddingElementOverwriteAndNoOverwrite(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 1000
	rand_strs := makeRandomStrings(num_elems, 1)
	rand_ints := makeRandomInts(num_elems, 100)

	num_unique_strs := len(uniqueElements(rand_strs))

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg *sync.WaitGroup) {
			if rand.Float64() < .5 {
				skiplist.Upsert(rand_strs[i], NewNoOverwriteCheck(&rand_ints[i]))
			} else {
				skiplist.Upsert(rand_strs[i], NewOverwriteCheck(&rand_ints[i]))
			}

			wg.Done()
		}(&wg)
	}

	wg.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	if len(elems) != num_unique_strs {
		t.Error("All elements not added/added incorrectly")
	}
}

func Test_ConcurrentAddingSameKey(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 1000
	rand_ints := makeRandomInts(num_elems, 100)

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg *sync.WaitGroup) {
			skiplist.Upsert("hello", NewNoOverwriteCheck(&rand_ints[i]))
			wg.Done()
		}(&wg)
	}

	wg.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	new_int_list := make([]int, 0)
	for _, elem := range elems {
		new_int_list = append(new_int_list, *elem)
	}

	sort.Ints(new_int_list)
	sort.Ints(rand_ints)
	if len(elems) != 1 {
		t.Error("Only one element should have been added")
	}
}

func Test_ConcurrentAddingAndDeleting(t *testing.T) {
	// "\U0010FFFF" is the highest value unicode character
	skiplist := NewSkipList[string, int](10, "", "\U0010FFFF")

	num_elems := 100
	rand_strs := makeRandomStrings(num_elems, 1)
	rand_ints := makeRandomInts(num_elems, 100)

	var wg sync.WaitGroup

	wg.Add(num_elems)

	for i := 0; i < num_elems; i++ {
		go func(wg *sync.WaitGroup) {
			if makeRandomInts(1, 100)[0] < 50 {
				skiplist.Upsert(rand_strs[i], NewNoOverwriteCheck(&rand_ints[i]))
			} else {
				skiplist.Delete(rand_strs[i])
			}

			wg.Done()
		}(&wg)
	}

	wg.Wait()

	elems, _ := skiplist.Query("", "\U0010FFFF")

	elem_list := make([]int, 0)

	for _, elem := range elems {
		elem_list = append(elem_list, *elem)
	}

	fmt.Println(len(elem_list))

	if !isSubset(elem_list, rand_ints) {
		t.Error("Corrupted data when inserting and removing")
	}

	skiplist.Visualize()
}
