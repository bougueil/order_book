# OrderBook

** order_book exercise
implements as best the exercise with persistence using :mnesia

** limits
- limited tests
- some operations should be in transaction, skipped
- example test passed 

** persistence
to reset the database do
rm -rf Mnesia.nonode@nohost

** running the exemple
```elixir
iex -S mix
	Exchange.test()
```

** examine the db
You should see the ~W(bid ask log)a tables
```elixir
	OrderBook.DB.dump 10 # dump up to 10 elements for the 3 table
```

