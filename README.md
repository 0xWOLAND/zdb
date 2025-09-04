`zbd`

This is an educational implementation of a B+ tree database inspired by [redb](https://github.com/cberner/redb) written in Zig. It similarly uses OS-level CoW. 

### Usage

```bash
# Run the demo
just demo 

# Run the benchmark 
just bench
```

### Benchmarks
Comparison of in-memory vs persistent pager
| Operation | Memory | Pager |
|-----------|---------|--------|
| Write | 3ns/op | 5,361ns/op |
| Read | - | 827ns/op |

### TODO
- [] SQL parser
- [] MVCC
- [] concurrency
