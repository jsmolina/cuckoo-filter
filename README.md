# cuckoo-filter

Derived implementation from https://github.com/efficient/cuckoofilter 
and https://jsfiddle.net/nojoL16o/2/

Original paper in: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf

# TODO
* Lua-Redis implementation  (http://redis.io/commands/EVAL#bitop http://blog.jupo.org/2013/06/12/bitwise-lua-operations-in-redis/)
* Bring Packedtable from C.
* EVAL perf with pytest-benchmark