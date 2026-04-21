def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument("--n-shards", type=int, required=True)
    parser.add_argument("--days-back", type=int, default=35)
    args = parser.parse_args()

    ensure_table()

    symbols = get_sp500_symbols()
    symbols = [s.strip().upper() for s in symbols if s and isinstance(s, str)]
    my_symbols = [s for s in symbols if stable_shard(s, args.n_shards) == args.shard_id]

    print(f"[INFO] shard={args.shard_id}/{args.n_shards} symbols={len(my_symbols)} days_back={args.days_back}")

    # ✅ now uses shard-level two-phase: defs batched, data batched
    run_shard_two_phase(my_symbols, days_back=args.days_back)


if __name__ == "__main__":
    main()