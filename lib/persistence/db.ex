defmodule OrderBook.DB do
  alias :mnesia, as: Mnesia
  use GenServer
  require Logger
  @tables ~W(bid ask log)a

  @moduledoc """
  Order Book Persistence api.

  Mnesia is used for storing 3 tables:

  bid, ask and logs

  Tables share the common structure : store key (:price_level), value
  """

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  """
  @impl true
  def init([]) do
    tables = Mnesia.system_info(:local_tables)

    case tables do
      [:schema] ->
        Mnesia.stop()
        Mnesia.create_schema([Node.self()])
        Mnesia.start()

        for table <- @tables do
          Mnesia.create_table(table,
	    type: :ordered_set,
	    attributes: [:price_level, :current],
            disc_copies: [Node.self()]
          )
        end

      [_ | _] ->
        wait_for_tables()
        :ok
    end

    wait_for_tables()
  end

  defp wait_for_tables do
    tables =
      for t <- Mnesia.system_info(:local_tables) do
        {t, Mnesia.table_info(t, :disc_copies)}
      end

    case Mnesia.wait_for_tables(Mnesia.system_info(:local_tables), 20000) do
      :ok ->
        {:ok, []}

      err ->
        Logger.error(
          "wait_for_tables, #{inspect(err)} - Couldn't load mnesia tables: (#{inspect(length(tables))}) #{inspect(tables)},  CHECK cookie:#{inspect(Node.get_cookie())}"
        )

        {err, []}
    end
  end

  @doc """
  only for debug
  """
  def dump(number_records \\ 100) do
    for table <- @tables do
      all = :ets.tab2list(table)
      list = Enum.take(all, number_records) |> Enum.sort()

      IO.puts(
        "#{table}: dump #{min(number_records, length(all))} items out of #{inspect(length(all))} items."
      )

      Enum.each(
        list,
        fn {^table, key, elem} ->
          IO.puts("#{key}: #{inspect(elem)} ")
        end
      )
    end

    :ok
  end

  @doc """
  Write an vent as a log in the db
  """
  def write_log(event) do
    _ = Mnesia.dirty_write({:log, System.monotonic_time(), event})
    :ok
  end

  @doc """
  Write Bid or Ask in their table
  """
  def write(bid_or_ask, price_level, price_qty) do
    _ = Mnesia.dirty_write({bid_or_ask, price_level, price_qty})
    :ok
  end

  @doc """
  Existing price levels with a greater or equal index are shifted up
  """
  def write_shift_up(bid_or_ask, price_level, new_price_qty) do
    upper_levels =
      :mnesia.dirty_select(
        bid_or_ask,
        [{{bid_or_ask, :"$1", :"$2"}, [{:>=, :"$1", price_level}], [{{:"$1", :"$2"}}]}]
      )

    # transaction HERE 
    Mnesia.dirty_write({bid_or_ask, price_level, new_price_qty})

    for {level, data} <- upper_levels do
      Mnesia.dirty_write({bid_or_ask, level + 1, data})
      Mnesia.dirty_delete(bid_or_ask, level)
    end

    :ok
  end

  @doc """
  Delete a price level. Existing price levels with a higher index will be shifted down
  """
  def delete_shift_down(bid_or_ask, price_level) do
    upper_levels =
      Mnesia.dirty_select(
        bid_or_ask,
        [{{bid_or_ask, :"$1", :"$2"}, [{:>, :"$1", price_level}], [{{:"$1", :"$2"}}]}]
      )

    # transaction HERE 
    Mnesia.dirty_delete(bid_or_ask, price_level)
    for {level, data} <- upper_levels do
      Mnesia.dirty_write({bid_or_ask, level - 1, data})
      Mnesia.dirty_delete(bid_or_ask, level)
    end
    :ok
  end

  @doc """
  read book at level: level
  we will only considering price levels that are less than or equal (le) than the specified book_depth.
  """
  def read_le(bid_or_ask, book_depth) do
    Mnesia.dirty_select(
      bid_or_ask,
      [{{bid_or_ask, :"$1", :"$2"}, [{:"=<", :"$1", book_depth}], [{{:"$1", :"$2"}}]}]
    )
  end

  @doc """
  read book at level: level
  """
  def read(bid_or_ask, level) do
    Mnesia.dirty_read(bid_or_ask, level)
  end
end
