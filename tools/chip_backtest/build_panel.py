"""Step 2: forward-outcome panel (needs px.parquet from dump.py) -> panel.parquet; then run feat.py -> feat.parquet."""
import bt
px = bt.load_px(); res = bt.build_outcomes(px); res = bt.add_liquidity(px, res); res = bt.add_excess(res)
res.to_parquet(f'{bt.S}/panel.parquet'); print(res.shape)
