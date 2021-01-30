defmodule Exchange do
  use GenServer
  require Logger

  @moduledoc """
  Exchange as defined in the example
  """

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init([]) do
    {:ok, []}
  end

  # @spec send_instruction(exchange_pid: pid(),    event: map() ) :: :ok | {:error, any()}
  def send_instruction(exchange_pid, %{instruction: :update} = event) do
    GenServer.call(exchange_pid, {:send_instruction, event})
  end

  def send_instruction(exchange_pid, %{instruction: ins} = event) when ins in ~w(new delete)a do
    GenServer.cast(exchange_pid, {:send_instruction, event})
  end

  # @spec order_book(
  #   exchange: pid(),
  #   book_depth: integer()
  # ) :: list(map())
  @doc """
  we will only considering price levels that are less than or equal than the specified `book_depth
  """
  def order_book(pid, book_depth) do
    {bids, asks} = GenServer.call(pid, {:order_book, book_depth})

    # we could avoid this re-calculation
    {min_bids, _} = Enum.min(bids)
    {max_bids, _} = Enum.max(bids)
    {min_asks, _} = Enum.min(asks)
    {max_asks, _} = Enum.max(asks)

    min1 = min(min_bids, min_asks)
    max1 = max(max_bids, max_asks) |> max(book_depth)

    bids_map = Enum.into(bids, %{})
    asks_map = Enum.into(asks, %{})

    Enum.reduce(min1..max1, [], fn pos, acc ->
      {bid_price, bid_qty} = Map.get(bids_map, pos, {0.0, 0})
      {ask_price, ask_qty} = Map.get(asks_map, pos, {0.0, 0})

      [
        %{
          bid_price: bid_price,
          bid_quantity: bid_qty,
          ask_price: ask_price,
          ask_quantity: ask_qty
        }
        | acc
      ]
    end)
    |> Enum.reverse()
  end

  @impl true
  def handle_call({:order_book, book_depth}, _from, state) do
    bids = OrderBook.DB.read_le(:bid, book_depth)
    asks = OrderBook.DB.read_le(:ask, book_depth)
    {:reply, {bids, asks}, state}
  end

  @impl true
  def handle_call({:send_instruction, %{instruction: :update, side: side} = event}, _from, state) do
    reply =
      case OrderBook.DB.read(side, event.price_level_index) do
        [] ->
          {:error, :not_found}

        [{_, _, _}] ->
          OrderBook.DB.write(side, event.price_level_index, {event.price, event.quantity})
          OrderBook.DB.write_log(event)
      end

    {:reply, reply, state}
  end

  @impl true
  # Insert new price level. Existing price levels with a greater or equal index are shifted up
  def handle_cast({:send_instruction, %{instruction: :new, side: side} = event}, state) do
    case OrderBook.DB.read(side, event.price_level_index) do
      [] ->
        OrderBook.DB.write(side, event.price_level_index, {event.price, event.quantity})
        OrderBook.DB.write_log(event)

      [{_, _, _}] ->
        OrderBook.DB.write_shift_up(side, event.price_level_index, {event.price, event.quantity})
        OrderBook.DB.write_log(event)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:send_instruction, %{instruction: :delete, side: side} = event}, state) do
    case OrderBook.DB.read(side, event.price_level_index) do
      [] ->
        :ok

      [{_, _, _}] ->
        OrderBook.DB.delete_shift_down(side, event.price_level_index)
        OrderBook.DB.write_log(event)
    end

    {:noreply, state}
  end

  def test() do
    exchange_pid = GenServer.whereis(Exchange)

    :ok =
      Exchange.send_instruction(exchange_pid, %{
        instruction: :new,
        side: :bid,
        price_level_index: 1,
        price: 50.0,
        quantity: 30
      })

    :ok =
      Exchange.send_instruction(exchange_pid, %{
        instruction: :new,
        side: :bid,
        price_level_index: 2,
        price: 40.0,
        quantity: 40
      })

    :ok =
      Exchange.send_instruction(exchange_pid, %{
        instruction: :new,
        side: :ask,
        price_level_index: 1,
        price: 60.0,
        quantity: 10
      })

    :ok =
      Exchange.send_instruction(exchange_pid, %{
        instruction: :new,
        side: :ask,
        price_level_index: 2,
        price: 70.0,
        quantity: 10
      })

    :ok =
      Exchange.send_instruction(exchange_pid, %{
        instruction: :update,
        side: :ask,
        price_level_index: 2,
        price: 70.0,
        quantity: 20
      })

    :ok =
      Exchange.send_instruction(exchange_pid, %{
        instruction: :update,
        side: :bid,
        price_level_index: 1,
        price: 50.0,
        quantity: 40
      })

    Exchange.order_book(exchange_pid, 2)
  end
end
