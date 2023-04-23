package types

import (
	"fmt"

	shares "github.com/rollkit/rollkit/shares"
	pb "github.com/rollkit/rollkit/types/pb/rollkit"
	"github.com/tendermint/tendermint/crypto/merkle"
	"github.com/tendermint/tendermint/crypto/tmhash"
	tmbytes "github.com/tendermint/tendermint/libs/bytes"
)

// Tx represents transactoin.
type Tx []byte

// Txs represents a slice of transactions.
type Txs []Tx

// Hash computes the TMHASH hash of the wire encoded transaction.
func (tx Tx) Hash() []byte {
	return tmhash.Sum(tx)
}

// Proof returns a simple merkle proof for this node.
// Panics if i < 0 or i >= len(txs)
// TODO: optimize this!
func (txs Txs) Proof(i int) TxProof {
	l := len(txs)
	bzs := make([][]byte, l)
	for i := 0; i < l; i++ {
		bzs[i] = txs[i].Hash()
	}
	root, proofs := merkle.ProofsFromByteSlices(bzs)

	return TxProof{
		RootHash: root,
		Data:     txs[i],
		Proof:    *proofs[i],
	}
}

// TxProof represents a Merkle proof of the presence of a transaction in the Merkle tree.
type TxProof struct {
	RootHash tmbytes.HexBytes `json:"root_hash"`
	Data     Tx               `json:"data"`
	Proof    merkle.Proof     `json:"proof"`
}

func (txs Txs) ToTxsWithISRs(intermediateStateRoots IntermediateStateRoots) ([]pb.TxWithISRs, error) {
	expectedLength := len(txs) + 2
	if len(intermediateStateRoots.RawRootsList) != len(txs)+2 {
		return nil, fmt.Errorf("invalid length of ISR list: %d, expected length: %d", len(intermediateStateRoots.RawRootsList), expectedLength)
	}
	txsWithISRs := make([]pb.TxWithISRs, 0)
	for i, tx := range txs {
		txsWithISRs = append(txsWithISRs, pb.TxWithISRs{
			PreIsr:  intermediateStateRoots.RawRootsList[i],
			Tx:      tx,
			PostIsr: intermediateStateRoots.RawRootsList[i+1],
		})
	}
	return txsWithISRs, nil
}

func TxsWithISRsToShares(txsWithISRs []pb.TxWithISRs) (txShares []shares.Share, err error) {
	byteSlices := make([][]byte, len(txsWithISRs))
	for i, txWithISR := range txsWithISRs {
		byteSlices[i], err = txWithISR.Marshal()
		if err != nil {
			return nil, err
		}
	}
	coreTxs := shares.TxsFromBytes(byteSlices)
	txShares, err = shares.SplitTxs(coreTxs)
	return txShares, err
}

func SharesToTxsWithISRs(txShares []shares.Share) (txsWithISRs []pb.TxWithISRs, err error) {
	byteSlices, err := shares.ParseCompactShares(txShares)
	if err != nil {
		return nil, err
	}
	txsWithISRs = make([]pb.TxWithISRs, len(byteSlices))
	for i, byteSlice := range byteSlices {
		var txWithISR pb.TxWithISRs
		err = txWithISR.Unmarshal(byteSlice)
		if err != nil {
			return nil, err
		}
		txsWithISRs[i] = txWithISR
	}
	return txsWithISRs, nil
}
