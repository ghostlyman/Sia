package transactionpool

import (
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
)

// purge removes all transactions from the transaction pool.
func (tp *TransactionPool) purge() {
	tp.knownObjects = make(map[ObjectID]TransactionSetID)
	tp.transactionSets = make(map[TransactionSetID][]types.Transaction)
	tp.transactionSetDiffs = make(map[TransactionSetID]modules.ConsensusChange)
	tp.transactionListSize = 0
}

// ProcessConsensusChange gets called to inform the transaction pool of changes
// to the consensus set.
func (tp *TransactionPool) ProcessConsensusChange(cc modules.ConsensusChange) {
	tp.mu.Lock()

	// Update the database of confirmed transactions.
	var revertedSets [][]types.Transaction
	for _, block := range cc.RevertedBlocks {
		revertedSets = append(revertedSets, block.Transactions)
		for _, txn := range block.Transactions {
			err := tp.deleteTransaction(tp.dbTx, txn.ID())
			if err != nil {
				tp.log.Println("ERROR: could not delete a transaction:", err)
			}
		}
	}
	var appliedSets [][]types.Transaction
	for _, block := range cc.AppliedBlocks {
		appliedSets = append(appliedSets, block.Transactions)
		for _, txn := range block.Transactions {
			err := tp.addTransaction(tp.dbTx, txn.ID())
			if err != nil {
				tp.log.Println("ERROR: could not add a transaction:", err)
			}
		}
	}
	appliedAndRevertedSets := append(appliedSets, revertedSets...)

	err := tp.putRecentConsensusChange(tp.dbTx, cc.ID)
	if err != nil {
		tp.log.Println("ERROR: could not update the recent consensus change:", err)
	}

	// try adding every transaction in the consensus change back into the pool
	// one at a time, starting with the oldest.
	var validTransactions [][]types.Transaction
	for i := len(appliedAndRevertedSets) - 1; i >= 0; i-- {
		var validSet []types.Transaction
		tSet := appliedAndRevertedSets[i]
		for j := len(tSet) - 1; j >= 0; j-- {
			txn := tSet[j]
			err = tp.acceptTransactionSet([]types.Transaction{txn}, cc.TryTransactionSet)
			if err == nil || err == modules.ErrDuplicateTransactionSet {
				validSet = append(validSet, txn)

				// if this transaction is valid, make sure it gets added back to the
				// database of confirmed transactions, since it could have been in a
				// reverted block.
				err = tp.addTransaction(tp.dbTx, txn.ID())
				if err != nil {
					tp.log.Println("ERROR: could not add a transaction:", err)
				}
			}
		}
		validTransactions = append(validTransactions, validSet)
	}

	// construct a map of transaction ids, containing every valid confirmed
	// transaction id in the ConsensusChange.
	txids := make(map[types.TransactionID]struct{})
	for _, txnSet := range validTransactions {
		for _, txn := range txnSet {
			txids[txn.ID()] = struct{}{}
		}
	}
	// Save all of the current unconfirmed transaction sets into a list.
	var unconfirmedSets [][]types.Transaction
	for _, tSet := range tp.transactionSets {
		// Compile a new transaction set the removes all transactions duplicated
		// in the block. Though mostly handled by the dependency manager in the
		// transaction pool, this should both improve efficiency and will strip
		// out duplicate transactions with no dependencies (arbitrary data only
		// transactions)
		var newTSet []types.Transaction
		for _, txn := range tSet {
			_, exists := txids[txn.ID()]
			if !exists {
				newTSet = append(newTSet, txn)
			}
		}
		unconfirmedSets = append(unconfirmedSets, newTSet)
	}

	// Purge the transaction pool. Some of the transactions sets may be invalid
	// after the consensus change.
	tp.purge()

	// Add all of the unconfirmed transaction sets back to the transaction
	// pool. The ones that are invalid will throw an error and will not be
	// re-added.
	//
	// Accepting a transaction set requires locking the consensus set (to check
	// validity). But, ProcessConsensusChange is only called when the consensus
	// set is already locked, causing a deadlock problem. Therefore,
	// transactions are readded to the pool in a goroutine, so that this
	// function can finish and consensus can unlock. The tpool lock is held
	// however until the goroutine completes.
	//
	// Which means that no other modules can require a tpool lock when
	// processing consensus changes. Overall, the locking is pretty fragile and
	// more rules need to be put in place.
	for _, set := range unconfirmedSets {
		tp.acceptTransactionSet(set, cc.TryTransactionSet) // Error is not checked.
	}

	// Inform subscribers that an update has executed.
	tp.mu.Demote()
	tp.updateSubscribersTransactions()
	tp.mu.DemotedUnlock()
}

// PurgeTransactionPool deletes all transactions from the transaction pool.
func (tp *TransactionPool) PurgeTransactionPool() {
	tp.mu.Lock()
	tp.purge()
	tp.mu.Unlock()
}
