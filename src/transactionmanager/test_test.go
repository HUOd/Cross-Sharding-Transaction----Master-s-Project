package transactionmanager

import "cst/src/shardkv"

import "testing"
import "strconv"

import "time"
import "fmt"

func check(t *testing.T, ck *shardkv.Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

func check_b(b *testing.B, ck *shardkv.Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		b.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

func TestBasicFunctions(t *testing.T) {
	fmt.Printf("Test: Basic functions ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeShardKVClient()

	cfg.join(0)
	cfg.join(1)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	tck := cfg.tmanagerclerk()
	// t1
	t1, _ := tck.Begin()
	v0, err := tck.Get(ka[0], t1)
	if err != OK {
		t.Fatalf("Got err %v from t %d. \n", err, t1)
	}
	v0 += randstring(20)
	err = tck.Put(ka[0], v0, t1)
	if err != OK {
		t.Fatalf("Got err %v from t %d. \n", err, t1)
	}
	err = tck.Commit(t1)
	if err != OK {
		t.Fatalf("Got err %v from t %d. \n", err, t1)
	}

	check(t, ck, ka[0], v0)

	ck.Put(ka[0], va[0])

	// t2 with master move
	t2, _ := tck.Begin()
	v0, err = tck.Get(ka[0], t2)
	if err != OK {
		t.Fatalf("Got err %v from t %d. \n", err, t2)
	}
	v0 += randstring(20)
	err = tck.Put(ka[0], v0, t2)
	if err != OK {
		t.Fatalf("Got err %v from t %d. \n", err, t2)
	}
	cfg.leave(1)
	err = tck.Commit(t2)
	if err != OK && err != ErrConfigChanged {
		t.Fatalf("Got err %v from t %d. \n", err, t2)
	}

	// check(t, ck, ka[0], va[0])

	cfg.join(1)
	time.Sleep(2000 * time.Millisecond)

	check(t, ck, ka[0], va[0])

	// many Trnasactions with commit on different keys
	nChan := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(ix int) {
			tck := cfg.tmanagerclerk()
			tx, _ := tck.Begin()
			newV, err := tck.Get(ka[ix], tx)
			if err != OK {
				t.Fatalf("Got err %v from t %d. \n", err, tx)
			}

			newV += randstring(20)

			err = tck.Put(ka[ix], newV, tx)
			if err != OK {
				t.Fatalf("Got err %v from t %d. \n", err, tx)
			}

			err = tck.Commit(tx)

			if err != OK {
				t.Fatalf("Got err %v from t %d. \n", err, tx)
			}

			nChan <- struct{}{}
			check(t, ck, ka[ix], newV)
		}(i)

		// go func(ix int, mChan chan string) {
		// 	ckx := cfg.makeShardKVClient()
		// 	newV := <-mChan
		// 	v := ckx.Get(ka[ix])
		// 	if v != newV && v != va[ix] {
		// 		t.Fatalf("%d: Got value not after transaction, not atomic. \n", ix)
		// 	}
		// 	nChan <- struct{}{}
		// }(i, mmChan)
	}

	total := n
	for total > 0 {
		<-nChan
		total--
		fmt.Printf("Total: %d \n", total)
	}

	fmt.Printf("  ... Passed\n")
}

func BenchmarkReadOnlyTransactions(b *testing.B) {
	fmt.Printf("Test: ReadOnly Transactions ...\n")

	cfg := make_config_b(b, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeShardKVClient()

	cfg.join(0)
	cfg.join(1)
	cfg.join(2)
	// time.Sleep(1 * time.Second)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}

	for i := 0; i < n; i++ {
		check_b(b, ck, ka[i], va[i])
	}

	// many Trnasactions with commit on different keys
	nChan := make(chan struct{})
	b.N = 20
	for i := 0; i < b.N; i++ {
		// mmChan := make(chan string)
		go func(ix int) {
			tck := cfg.tmanagerclerk()
			tx, _ := tck.Begin()
			index := ix % n
			_, err := tck.Get(ka[index], tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			err = tck.Commit(tx)
			if err != OK {
				if err == ErrAbort {
					nChan <- struct{}{}
					return
				}
				b.Fatalf("Got err %v from t %d. \n", err, tx)
			}

			// check_b(b, ck, ka[ix], va[ix])
			nChan <- struct{}{}
		}(i)
	}

	total := b.N
	for total > 0 {
		<-nChan
		total--
		fmt.Printf("Total: %d \n", total)
	}

	fmt.Printf("  ... Passed\n")
}

func BenchmarkReadWriteTransactions(b *testing.B) {
	fmt.Printf("Test: ReadWrite Transactions ...\n")

	cfg := make_config_b(b, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeShardKVClient()

	cfg.join(0)
	cfg.join(1)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}

	for i := 0; i < n; i++ {
		check_b(b, ck, ka[i], va[i])
	}

	// many Trnasactions with commit on different keys
	b.N = 20
	nChan := make(chan struct{})
	for i := 0; i < b.N; i++ {
		// mmChan := make(chan string)
		go func(ix int) {
			tck := cfg.tmanagerclerk()
			tx, _ := tck.Begin()
			index := ix % n
			newV, err := tck.Get(ka[index], tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			newV += randstring(20)

			err = tck.Put(ka[index], newV, tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			//fmt.Printf("Try Commit Transaction %d.\n", tx)
			err = tck.Commit(tx)
			// fmt.Printf("Committed Transaction %d completed!\n", tx)

			if err != OK {
				if err == ErrAbort {
					nChan <- struct{}{}
					return
				}
				b.Fatalf("Got err %v from t %d. \n", err, tx)
			}

			nChan <- struct{}{}
			// check_b(b, ck, ka[ix], newV)
			// ck.Put(ka[index], va[index])
		}(i)
	}

	total := b.N
	for total > 0 {
		<-nChan
		total--
		fmt.Printf("Total: %d \n", total)
	}

	fmt.Printf("  ... Passed\n")
}

func BenchmarkRWTransactionsShards(b *testing.B) {
	fmt.Printf("Test: ReadWrite Transactions ...\n")

	cfg := make_config_b(b, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeShardKVClient()

	cfg.join(0)
	cfg.join(1)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}

	for i := 0; i < n; i++ {
		check_b(b, ck, ka[i], va[i])
	}

	// many Trnasactions with commit on different keys
	b.N = 20
	nChan := make(chan struct{})
	for i := 0; i < b.N; i++ {
		// mmChan := make(chan string)
		go func(ix int) {
			tck := cfg.tmanagerclerk()
			tx, _ := tck.Begin()
			index := ix % n
			newV, err := tck.Get(ka[index], tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			newV += randstring(20)

			err = tck.Put(ka[index], newV, tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			index = (index + 1) % n
			newV, err = tck.Get(ka[index], tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			newV += randstring(20)

			err = tck.Put(ka[index], newV, tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			index = (index + 2) % n
			newV, err = tck.Get(ka[index], tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			newV += randstring(20)

			err = tck.Put(ka[index], newV, tx)
			if err == ErrAbort {
				nChan <- struct{}{}
				return
			}

			fmt.Printf("Try Commit Transaction %d.\n", tx)
			err = tck.Commit(tx)
			fmt.Printf("Committed Transaction %d completed!\n", tx)

			if err != OK {
				if err == ErrAbort {
					nChan <- struct{}{}
					return
				}
				b.Fatalf("Got err %v from t %d. \n", err, tx)
			}

			nChan <- struct{}{}
			// check_b(b, ck, ka[ix], newV)
			// ck.Put(ka[index], va[index])
		}(i)
	}

	total := b.N
	for total > 0 {
		<-nChan
		total--
		fmt.Printf("Total: %d \n", total)
	}

	fmt.Printf("  ... Passed\n")
}
