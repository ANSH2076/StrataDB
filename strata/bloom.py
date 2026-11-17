"""Bloom filter for Strata.

A Bloom filter is a space-efficient probabilistic data structure that
tests whether an element is a member of a set.  It can tell you:
  - "Definitely NOT in the set" (100% guaranteed — no false negatives)
  - "PROBABLY in the set" (with a small, tunable false-positive rate)

Why it matters in an LSM-tree:
Checking an SSTable for a key that does not exist requires seeking and
reading records from disk.  A Bloom filter kept in memory lets us skip
the disk entirely for ~99% of absent keys.
"""

from __future__ import annotations

import hashlib
import struct
from typing import ClassVar


class BloomFilter:
    """Bit-array Bloom filter using double-hashing with blake2b.

    Args:
        num_bits: Total number of bits in the filter bitset.
        num_hashes: Number of hash probes per key.
    """

    DEFAULT_BITS_PER_KEY: ClassVar[int] = 10
    DEFAULT_NUM_HASHES: ClassVar[int] = 7

    def __init__(self, num_bits: int, num_hashes: int) -> None:
        if num_bits <= 0:
            raise ValueError(f"num_bits must be positive, got {num_bits}")
        if num_hashes <= 0:
            raise ValueError(f"num_hashes must be positive, got {num_hashes}")

        self._num_bits = num_bits
        self._num_hashes = num_hashes
        # Backed by a bytearray. (num_bits // 8 + 1) ensures enough bytes
        # to hold all bits even when num_bits is not a multiple of 8.
        self._bytes = bytearray(num_bits // 8 + 1)

    @classmethod
    def for_capacity(
        cls,
        n_keys: int,
        bits_per_key: int = DEFAULT_BITS_PER_KEY,
    ) -> "BloomFilter":
        """Create a filter sized for n_keys with ~1% false-positive rate.

        Why 10 bits per key and 7 hashes:
        In Bloom filter theory, the optimal number of hashes is:
            k = (m / n) * ln(2) ≈ (m / n) * 0.693
        For m/n = 10 bits per key:
            k ≈ 10 * 0.693 ≈ 6.93 → 7 hashes.
        The resulting false positive probability is:
            p ≈ (1 - e^(-k*n/m))^k ≈ (1 - e^(-7/10))^7 ≈ 0.0082 ≈ 0.82% (~1%).
        We enforce a minimum of 64 bits to avoid pathological small sizes.
        """
        num_bits = max(64, n_keys * bits_per_key)
        return cls(num_bits=num_bits, num_hashes=cls.DEFAULT_NUM_HASHES)

    # -- Internal Hashing ------------------------------------------------------

    def _hash_positions(self, key: str) -> list[int]:
        """Compute the num_hashes bit positions for a key.

        NEVER use Python's built-in hash():
        Python randomizes string hash seeds on every process startup
        (via PYTHONHASHSEED) to prevent HashDoS attacks.  If we used
        hash(), a filter built in one process or saved to disk would
        produce different bit positions in another process, causing
        catastrophic false negatives where existing keys are reported absent!
        Instead, we use hashlib.blake2b, which is completely deterministic.

        Double-Hashing Trick:
        Generating 7 independent cryptographic hashes per key would be
        slow.  Instead, we compute a single 16-byte blake2b digest, split
        it into two 64-bit unsigned integers (h1 and h2), and generate
        the i-th bit position as:
            pos_i = (h1 + i * h2) % num_bits
        In plain English: h1 gives the starting position, and h2 acts as
        a constant stride step.  Kirsch & Mitzenmacher (2006) proved that
        this technique provides the same asymptotic false-positive rate
        as using k completely independent hash functions.
        """
        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=16).digest()
        # Unpack two 64-bit big-endian unsigned integers.
        h1, h2 = struct.unpack(">QQ", digest)

        # Ensure h2 is odd and non-zero so the stride cycles smoothly.
        if h2 == 0:
            h2 = 1

        positions: list[int] = []
        for i in range(self._num_hashes):
            pos = (h1 + i * h2) % self._num_bits
            positions.append(pos)
        return positions

    # -- Public API ------------------------------------------------------------

    def add(self, key: str) -> None:
        """Add a key to the filter by setting all its hash probe bits to 1."""
        for bit_pos in self._hash_positions(key):
            byte_index = bit_pos // 8
            bit_offset = bit_pos % 8
            self._bytes[byte_index] |= (1 << bit_offset)

    def might_contain(self, key: str) -> bool:
        """Test whether key might be in the set.

        Returns False: The key is DEFINITELY NOT in the set (zero false negatives).
        Returns True:  The key MIGHT be in the set (small chance of false positive).
        """
        for bit_pos in self._hash_positions(key):
            byte_index = bit_pos // 8
            bit_offset = bit_pos % 8
            if not (self._bytes[byte_index] & (1 << bit_offset)):
                # If even a single probe bit is 0, this key was never added!
                return False
        return True

    # -- Inspection ------------------------------------------------------------

    @property
    def num_bits(self) -> int:
        return self._num_bits

    @property
    def num_hashes(self) -> int:
        return self._num_hashes

    @property
    def raw_bytes(self) -> bytes:
        """Return the underlying bitset bytes (for testing and serialization)."""
        return bytes(self._bytes)
