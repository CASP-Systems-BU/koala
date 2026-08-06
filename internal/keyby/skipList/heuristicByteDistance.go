package skipList

import (
	"strconv"
	"unicode"
)

// We implement the following heuristic byte distance functions:
// (1) LevenshteinDistance: Calculate the Levenshtein distance between two byte
// strings.
// (2) RollingHash: Calculate the rolling hash of a byte string.
// (3) TokenizedLevenshteinDistance: Compare the rolling hashes of two byte
// strings.

// LevenshteinDistance
func LevenshteinDistance(a, b []byte) int {
	m, n := len(a), len(b)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i := 0; i <= m; i++ {
		dp[i][0] = i
	}
	for j := 0; j <= n; j++ {
		dp[0][j] = j
	}
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])
			}
		}
	}
	return dp[m][n]
}

// RollingHash
func CompareHashes(a, b []byte, windowSize int) int {
	base, mod := 256, int(1e9+7)
	hashA := RollingHash(a[:windowSize], base, mod)
	hashB := RollingHash(b[:windowSize], base, mod)
	matches := 0
	if hashA == hashB {
		matches++
	}
	for i := windowSize; i < len(a) && i < len(b); i++ {
		hashA = (hashA*base - int(a[i-windowSize])*pow(base, windowSize, mod) + int(a[i]) + mod) % mod
		hashB = (hashB*base - int(b[i-windowSize])*pow(base, windowSize, mod) + int(b[i]) + mod) % mod
		if hashA == hashB {
			matches++
		}
	}
	return matches
}

// TokenizedLevenshteinDistance compares two byte strings by tokenizing them
// and calculating the Levenshtein distance between the tokens.
func TokenizedLevenshteinDistance(a, b []byte) float64 {
	tokensA := Tokenize(a)
	tokensB := Tokenize(b)
	return CompareTokens(tokensA, tokensB)
}

func CompareTokens(a, b [][]byte) float64 {
	score := 0.0
	maxLen := max(len(a), len(b))
	for i := 0; i < maxLen; i++ {
		if i >= len(a) || i >= len(b) {
			score += 1.0 // Penalize for mismatched length
			continue
		}
		tokenA, tokenB := a[i], b[i]
		if unicode.IsDigit(rune(tokenA[0])) &&
			unicode.IsDigit(rune(tokenB[0])) {
			// Numeric tokens: use absolute difference
			numA, _ := strconv.Atoi(string(tokenA))
			numB, _ := strconv.Atoi(string(tokenB))
			score += float64(abs(numA-numB)) / float64(max(numA, numB))
		} else {
			// Non-numeric tokens: use Levenshtein distance
			score += float64(LevenshteinDistance(tokenA, tokenB)) / float64(max(len(tokenA), len(tokenB)))
		}
	}
	return score / float64(maxLen)
}

// Utility functions
func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

func RollingHash(s []byte, base, mod int) int {
	hash := 0
	for _, v := range s {
		hash = (hash*base + int(v)) % mod
	}
	return hash
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func pow(base, exp, mod int) int {
	result := 1
	for exp > 0 {
		if exp%2 == 1 {
			result = (result * base) % mod
		}
		base = (base * base) % mod
		exp /= 2
	}
	return result
}

// Tokenize splits a byte string into numeric and non-numeric tokens.
func Tokenize(s []byte) [][]byte {
	var tokens [][]byte
	var current []byte
	for _, b := range s {
		if unicode.IsDigit(rune(b)) {
			if len(current) > 0 &&
				!unicode.IsDigit(rune(current[len(current)-1])) {
				tokens = append(tokens, current)
				current = nil
			}
		} else {
			if len(current) > 0 && unicode.IsDigit(rune(current[len(current)-1])) {
				tokens = append(tokens, current)
				current = nil
			}
		}
		current = append(current, b)
	}
	if len(current) > 0 {
		tokens = append(tokens, current)
	}
	return tokens
}
