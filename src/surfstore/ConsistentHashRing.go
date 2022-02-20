package surfstore

import (
    "crypto/sha256"
    "encoding/hex"
    "math/big"
)

type Node struct {
    Addr  string
    Index int
}

type ConsistentHashRing struct {
    RingSize int
    Nodes    []Node
}

// Perform a modulo operation on a hash string.
// The hash string is assumed to be hexadecimally encoded.
func HashMod(hashString string, ringSize int) int {
    hashBytes, _ := hex.DecodeString(hashString)
    hashInt := new(big.Int).SetBytes(hashBytes[:])
    ringSizeInt := big.NewInt(int64(ringSize))
    
    indexInt := new(big.Int).Mod(hashInt, ringSizeInt)
    
    return int(indexInt.Int64())
}

// Compute a block’s index on the ring from its hash value.
func (ms *ConsistentHashRing) ComputeBlockIndex(blockHash string) int {
    return HashMod(blockHash, ms.RingSize)
}

// Compute a node’s index on the ring from its address string.
func (ms *ConsistentHashRing) ComputeNodeIndex(nodeAddr string) int {
    hashBytes := sha256.Sum256([]byte(nodeAddr))
    hashString := hex.EncodeToString(hashBytes[:])
    return HashMod(hashString, ms.RingSize)
}

// Find the hosting node for the given ringIndex. It’s basically the first node on the ring with node.Index >= ringIndex (in a modulo sense).
func (ms *ConsistentHashRing) FindHostingNode(ringIndex int) Node {
    // Try to implement a O(log N) solution here using binary search.
    // It's also fine if you can't because we don't test your performance.
    ringIndex %= ms.RingSize
    var listId, minListId int
    hostIndex, minNodeIndex := ms.RingSize, ms.RingSize
    for i, node := range ms.Nodes {
        if node.Index < minNodeIndex { // store the node with the smallest index
            minNodeIndex, minListId = node.Index, i
        }
        if node.Index < hostIndex && node.Index >= ringIndex {
            hostIndex, listId = node.Index, i
        }
    }
    if hostIndex == ms.RingSize { // ringIndex is larger than any node's index
        hostIndex, listId = minNodeIndex, minListId
    }
    return ms.Nodes[listId]
}

func (ms *ConsistentHashRing) FindPredNode(queryIndex int) Node {
    var listId, maxListId int
    predIndex, maxNodeIndex := -1, -1
    for i, node := range ms.Nodes {
        if node.Index > maxNodeIndex {
            maxNodeIndex, maxListId = node.Index, i
        }
        if node.Index > predIndex && node.Index < queryIndex {
            predIndex, listId = node.Index, i
        }
    }
    if predIndex == -1 {
        predIndex, listId = maxNodeIndex, maxListId
    }
    return ms.Nodes[listId]
}

// Add the given nodeAddr to the ring.
func (ms *ConsistentHashRing) AddNode(nodeAddr string) {
    // O(N) solution is totally fine here.
    // O(log N) solution might be overly complicated.
    index := ms.ComputeNodeIndex(nodeAddr)
    node := Node{nodeAddr, index}
    ms.Nodes = append(ms.Nodes, node)
}

// Remove the given nodeAddr from the ring.
func (ms *ConsistentHashRing) RemoveNode(nodeAddr string) {
    // O(N) solution is totally fine here.
    // O(log N) solution might be overly complicated.
    if ms.Nodes[0].Addr == nodeAddr {
        ms.Nodes = ms.Nodes[1:]
        return
    }
    if ms.Nodes[len(ms.Nodes)-1].Addr == nodeAddr {
        ms.Nodes = ms.Nodes[:len(ms.Nodes)-1]
        return
    }
    for i, node := range ms.Nodes {
        if node.Addr == nodeAddr {
            ms.Nodes = append(ms.Nodes[:i], ms.Nodes[i+1:]...)
            return
        }
    }
}

// Create consistent hash ring struct with a list of blockstore addresses
func NewConsistentHashRing(ringSize int, blockStoreAddrs []string) ConsistentHashRing {
    // You can not use ComputeNodeIndex method to compute the ring index of blockStoreAddr in blockStoreAddrs here.
    // You will need to use HashMod function, remember to hash the blockStoreAddr before calling HashMod
    // Hint: refer to ComputeNodeIndex method on how to hash the blockStoreAddr before calling HashMod
    var nodes []Node
    for _, blockStoreAddr := range blockStoreAddrs {
        hashBytes := sha256.Sum256([]byte(blockStoreAddr))
        hashString := hex.EncodeToString(hashBytes[:])
        index := HashMod(hashString, ringSize)
        node := Node{blockStoreAddr, index}
        nodes = append(nodes, node)
    }
    return ConsistentHashRing{ringSize, nodes}
}
