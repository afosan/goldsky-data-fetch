from gql import gql
import pandas as pd
from utils import query_until_end


class Base:
    def __init__(self, client) -> None:
        self._client = client

    async def query_pools(self) -> "JSON":
        return await query_until_end(self._client, gql(self._query_get_pools()))

    def parse_pools_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["pools"])
        df["datetime"] = pd.to_datetime(df["timestamp"].astype(int), utc=True, unit="s")
        df.drop(columns="timestamp", inplace=True)

        return df

    def get_df_pools(self, df: pd.DataFrame) -> pd.DataFrame:
        df_pools = df.groupby(pd.Grouper(key="datetime", axis=0, freq="D")).agg(
            new_pool_count=("id", "count")
        )
        idx = pd.date_range(df_pools.index.min(), pd.Timestamp.utcnow())
        df_pools = df_pools.reindex(idx, fill_value=0)
        df_pools.sort_index(ascending=True, inplace=True)
        df_pools["total_pool_count"] = df_pools["new_pool_count"].cumsum()

        return df_pools

    def df_tokens(client, df: pd.DataFrame) -> pd.DataFrame:
        df_tokens = pd.concat(
            [
                df[["datetime", "token0.id"]].rename(columns={"token0.id": "token.id"}),
                df[["datetime", "token1.id"]].rename(columns={"token1.id": "token.id"}),
            ]
        ).reset_index()
        tmp = df_tokens.groupby("token.id").agg(first=("datetime", "min")).reset_index()
        token_first_deployments = dict(zip(tmp["token.id"], tmp["first"]))
        df_tokens["first"] = (
            df_tokens["token.id"].map(token_first_deployments) == df_tokens["datetime"]
        )
        df_tokens = df_tokens.groupby(pd.Grouper(key="datetime", axis=0, freq="D")).agg(
            new_token_count=("first", "sum")
        )
        idx = pd.date_range(df_tokens.index.min(), pd.Timestamp.utcnow())
        df_tokens = df_tokens.reindex(idx, fill_value=0)
        df_tokens.sort_index(ascending=True, inplace=True)
        df_tokens["total_token_count"] = df_tokens["new_token_count"].cumsum()

        return df_tokens

    async def query_exchange_day_data(self) -> "JSON":
        return await query_until_end(
            self._client, gql(self._query_get_exchange_day_data())
        )

    async def query_pool_day_data(self) -> "JSON":
        return await query_until_end(self._client, gql(self._query_get_pool_day_data()))

    async def query_swaps_data(self) -> "JSON":
        return await query_until_end(self._client, gql(self._query_get_swaps()))

    def get_df_exchange_day(
        self,
        df_exchange_day_data: pd.DataFrame,
        df_swaps_data: pd.DataFrame,
    ) -> pd.DataFrame:
        return df_exchange_day_data.join(df_swaps_data)

    def get_df_pool_day(
        self,
        df_pool_day_data: pd.DataFrame,
        df_swaps_data_by_pool: pd.DataFrame,
    ) -> pd.DataFrame:
        df_pool_day = pd.merge(
            df_pool_day_data,
            df_swaps_data_by_pool,
            how="left",
            left_on=["poolId", "date"],
            right_on=["pool_id", "date"],
        )
        df_pool_day.drop(columns=["poolId"], inplace=True)
        df_pool_day["totalVolumeUSD"] = df_pool_day.groupby(["pool_id"])[
            "dailyVolumeUSD"
        ].cumsum()
        df_pool_day["totalTransactions"] = df_pool_day.groupby(["pool_id"])[
            "dailyTransactions"
        ].cumsum()
        df_pool_day.set_index("date", inplace=True)
        return df_pool_day


class SupSwapExchangeV2(Base):
    def _query_get_pools(self) -> str:
        return """
        query getPools(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            pools: pairs(
                skip: $skip,
                first: $first,
            ) {
                id
                token0 {
                    id
                    name
                    symbol
                    decimals
                }
                token1 {
                    id
                    name
                    symbol
                    decimals
                }
                block
                timestamp
            }
        }
        """

    def _query_get_exchange_day_data(self) -> str:
        return """
        query getExchangeDayDatas(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            exchangeDayDatas: supDayDatas(
                skip: $skip,
                first: $first,
            ) {
                id
                date
                dailyVolumeETH
                dailyVolumeUSD
                dailyVolumeUntracked
                totalLiquidityETH
                totalLiquidityUSD
                # dailyFeeETH
                # dailyFeeUSD
                # dailyTransactions
                totalTransactions
            }
        }
        """

    def parse_exchange_day_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["exchangeDayDatas"])
        float_columns = [
            "dailyVolumeETH",
            "dailyVolumeUSD",
            "dailyVolumeUntracked",
            "totalLiquidityETH",
            "totalLiquidityUSD",
        ]
        int_columns = [
            "id",
            "date",
            "totalTransactions",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="s").dt.date
        df.sort_values(by="date", ascending=True, inplace=True)
        df["totalVolumeETH"] = df["dailyVolumeETH"].cumsum()
        df["totalVolumeUSD"] = df["dailyVolumeUSD"].cumsum()
        df["dailyTransactions"] = df["totalTransactions"] - df[
            "totalTransactions"
        ].shift(1).fillna(0).astype(int)
        df.set_index("date", inplace=True)
        df.index = pd.DatetimeIndex(df.index, tz="utc")

        return df

    def _query_get_pool_day_data(self) -> str:
        return """
        query getPoolDayDatas(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            poolDayDatas: pairDayDatas(
                skip: $skip,
                first: $first,
            ) {
                id
                date
                poolId: pairAddress
                # dailyVolumeETH
                dailyVolumeUSD
                # dailyVolumeUntracked
                # totalLiquidityETH
                totalLiquidityUSD: reserveUSD
                # dailyFeeETH
                # dailyFeeUSD
                dailyTransactions: dailyTxns
                # totalTransactions
            }
        }
        """

    def parse_pool_day_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["poolDayDatas"])
        float_columns = [
            "totalLiquidityUSD",
            "dailyVolumeUSD",
        ]
        int_columns = [
            "date",
            "dailyTransactions",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="s").dt.date

        return df

    def _query_get_swaps(self) -> str:
        return """
        query getSwaps(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            swaps: swaps(
                skip: $skip,
                first: $first,
            ) {
                id
                block: transaction {
                    block
                }
                timestamp
                poolId: pair {
                    id
                }
                from
                amountFeeUSD
            }
        }
        """

    def parse_swaps_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["swaps"])
        float_columns = [
            "amountFeeUSD",
        ]
        int_columns = [
            "timestamp",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["datetime"] = pd.to_datetime(df["timestamp"], utc=True, unit="s")
        df.drop(columns="timestamp", inplace=True)

        df = df.groupby(pd.Grouper(key="datetime", axis=0, freq="D")).agg(
            new_swap_count=("id", "count"),
            daily_fee_in_usd=("amountFeeUSD", "sum"),
        )
        idx = pd.date_range(df.index.min(), pd.Timestamp.utcnow())
        df = df.reindex(idx, fill_value=0)
        df.sort_index(ascending=True, inplace=True)
        df["total_swap_count"] = df["new_swap_count"].cumsum()
        df["total_fee_in_usd"] = df["daily_fee_in_usd"].cumsum()

        return df

    def parse_swaps_data_by_pool(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["swaps"])
        df.rename(
            columns={
                "block.block": "block",
                "poolId.id": "pool_id",
            },
            inplace=True,
        )
        float_columns = [
            "amountFeeUSD",
        ]
        int_columns = [
            "timestamp",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["timestamp"], utc=True, unit="s").dt.date
        df.drop(columns="timestamp", inplace=True)

        df = (
            df.groupby(
                [
                    "pool_id",
                    "date",
                ]
            )
            .agg(
                new_swap_count=("id", "count"),
                daily_fee_in_usd=("amountFeeUSD", "sum"),
            )
            .reset_index()
        )

        df["total_swap_count"] = df.groupby(["pool_id"])["new_swap_count"].cumsum()
        df["total_fee_in_usd"] = df.groupby(["pool_id"])["daily_fee_in_usd"].cumsum()

        return df


class SupSwapExchangeV3(Base):
    def _query_get_pools(self) -> str:
        return """
        query getPools(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            pools: pools(
                skip: $skip,
                first: $first,
            ) {
                id
                token0 {
                    id
                    name
                    symbol
                    decimals
                }
                token1 {
                    id
                    name
                    symbol
                    decimals
                }
                block: createdAtBlockNumber
                timestamp: createdAtTimestamp
            }
        }
        """

    def _query_get_exchange_day_data(self) -> str:
        return """
        query getExchangeDayDatas(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            exchangeDayDatas: supDayDatas(
                skip: $skip,
                first: $first,
            ) {
                id
                date
                dailyVolumeETH: volumeETH
                dailyVolumeUSD: volumeUSD
                dailyVolumeUntracked: volumeUSDUntracked
                # totalLiquidityETH
                totalLiquidityUSD: tvlUSD
                # dailyFeeETH
                dailyFeeUSD: feesUSD
                # dailyTransactions
                totalTransactions: txCount
            }
        }
        """

    def parse_exchange_day_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["exchangeDayDatas"])
        float_columns = [
            "dailyFeeUSD",
            "dailyVolumeETH",
            "dailyVolumeUSD",
            "dailyVolumeUntracked",
            "totalLiquidityUSD",
        ]
        int_columns = [
            "id",
            "date",
            "totalTransactions",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="s").dt.date
        df.sort_values(by="date", ascending=True, inplace=True)
        df["totalVolumeETH"] = df["dailyVolumeETH"].cumsum()
        df["totalVolumeUSD"] = df["dailyVolumeUSD"].cumsum()
        df["totalFeeUSD"] = df["dailyFeeUSD"].cumsum()
        df["dailyTransactions"] = df["totalTransactions"] - df[
            "totalTransactions"
        ].shift(1).fillna(0).astype(int)
        df.set_index("date", inplace=True)
        df.index = pd.DatetimeIndex(df.index, tz="utc")

        return df

    def _query_get_pool_day_data(self) -> str:
        return """
        query getPoolDayDatas(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            poolDayDatas: poolDayDatas(
                skip: $skip,
                first: $first,
            ) {
                id
                date
                poolId: pool {
                    id
                }
                # dailyVolumeETH
                dailyVolumeUSD: volumeUSD
                # dailyVolumeUntracked
                # totalLiquidityETH
                totalLiquidityUSD: tvlUSD
                # dailyFeeETH
                dailyFeeUSD: feesUSD
                dailyTransactions: txCount
                # totalTransactions
            }
        }
        """

    def parse_pool_day_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["poolDayDatas"])
        df.rename(columns={"poolId.id": "poolId"}, inplace=True)
        float_columns = [
            "totalLiquidityUSD",
            "dailyVolumeUSD",
            "dailyFeeUSD",
        ]
        int_columns = [
            "date",
            "dailyTransactions",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="s").dt.date

        return df

    def _query_get_swaps(self) -> str:
        return """
        query getSwaps(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            swaps: swaps(
                skip: $skip,
                first: $first,
            ) {
                id
                block: transaction {
                    blockNumber
                }
                timestamp
                poolId: pool {
                    id
                }
                from: origin
                amountFeeUSD
            }
        }
        """

    def parse_swaps_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["swaps"])
        float_columns = [
            "amountFeeUSD",
        ]
        int_columns = [
            "timestamp",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["datetime"] = pd.to_datetime(df["timestamp"], utc=True, unit="s")
        df.drop(columns="timestamp", inplace=True)

        df = df.groupby(pd.Grouper(key="datetime", axis=0, freq="D")).agg(
            new_swap_count=("id", "count"),
            daily_fee_in_usd=("amountFeeUSD", "sum"),
        )
        idx = pd.date_range(df.index.min(), pd.Timestamp.utcnow())
        df = df.reindex(idx, fill_value=0)
        df.sort_index(ascending=True, inplace=True)
        df["total_swap_count"] = df["new_swap_count"].cumsum()
        df["total_fee_in_usd"] = df["daily_fee_in_usd"].cumsum()

        return df

    def parse_swaps_data_by_pool(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["swaps"])
        df.rename(
            columns={
                "block.block": "block",
                "poolId.id": "pool_id",
            },
            inplace=True,
        )
        float_columns = [
            "amountFeeUSD",
        ]
        int_columns = [
            "timestamp",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["timestamp"], utc=True, unit="s").dt.date
        df.drop(columns="timestamp", inplace=True)

        df = (
            df.groupby(
                [
                    "pool_id",
                    "date",
                ]
            )
            .agg(
                new_swap_count=("id", "count"),
                daily_fee_in_usd=("amountFeeUSD", "sum"),
            )
            .reset_index()
        )

        df["total_swap_count"] = df.groupby(["pool_id"])["new_swap_count"].cumsum()
        df["total_fee_in_usd"] = df.groupby(["pool_id"])["daily_fee_in_usd"].cumsum()

        return df


class KimAmm(Base):
    def _query_get_pools(self) -> str:
        return """
        query getPools(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            pools: pairs(
                skip: $skip,
                first: $first,
            ) {
                id
                token0 {
                    id
                    name
                    symbol
                    decimals
                }
                token1 {
                    id
                    name
                    symbol
                    decimals
                }
                block: createdAtBlockNumber
                timestamp: createdAtTimestamp
            }
        }
        """

    def _query_get_exchange_day_data(self) -> str:
        return """
        query getExchangeDayDatas(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            exchangeDayDatas: uniswapDayDatas(
                skip: $skip,
                first: $first,
            ) {
                id
                date
                dailyVolumeETH
                dailyVolumeUSD
                dailyVolumeUntracked
                totalLiquidityETH
                totalLiquidityUSD
                dailyFeeETH
                dailyFeeUSD
                # dailyTransactions
                totalTransactions: txCount
            }
        }
        """

    def parse_exchange_day_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["exchangeDayDatas"])
        float_columns = [
            "dailyVolumeETH",
            "dailyVolumeUSD",
            "dailyVolumeUntracked",
            "dailyFeeETH",
            "dailyFeeUSD",
            "totalLiquidityETH",
            "totalLiquidityUSD",
        ]
        int_columns = [
            "id",
            "date",
            "totalTransactions",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="s").dt.date
        df.sort_values(by="date", ascending=True, inplace=True)
        df["totalVolumeETH"] = df["dailyVolumeETH"].cumsum()
        df["totalVolumeUSD"] = df["dailyVolumeUSD"].cumsum()
        df["totalFeeUSD"] = df["dailyFeeUSD"].cumsum()
        df["totalFeeETH"] = df["dailyFeeETH"].cumsum()
        df["dailyTransactions"] = df["totalTransactions"] - df[
            "totalTransactions"
        ].shift(1).fillna(0).astype(int)
        df.set_index("date", inplace=True)
        df.index = pd.DatetimeIndex(df.index, tz="utc")

        return df

    def _query_get_pool_day_data(self) -> str:
        return """
        query getPoolDayDatas(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            poolDayDatas: pairDayDatas(
                skip: $skip,
                first: $first,
            ) {
                id
                date
                poolId: pairAddress
                # dailyVolumeETH
                dailyVolumeUSD
                # dailyVolumeUntracked
                # totalLiquidityETH
                totalLiquidityUSD: reserveUSD
                # dailyFeeETH
                dailyFeeUSD
                dailyTransactions: dailyTxns
                # totalTransactions
            }
        }
        """

    def parse_pool_day_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["poolDayDatas"])
        float_columns = [
            "totalLiquidityUSD",
            "dailyVolumeUSD",
            "dailyFeeUSD",
        ]
        int_columns = [
            "date",
            "dailyTransactions",
        ]
        df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="s").dt.date

        return df

    def _query_get_swaps(self) -> str:
        return """
        query getSwaps(
            $skip: Int = 0,
            $first: Int = 1000,
        ) {
            swaps: swaps(
                skip: $skip,
                first: $first,
            ) {
                id
                block: transaction {
                    blockNumber
                }
                timestamp
                poolId: pair {
                    id
                }
                from
                # amountFeeUSD
            }
        }
        """

    def parse_swaps_data(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["swaps"])
        # float_columns = [
        #     "amountFeeUSD",
        # ]
        int_columns = [
            "timestamp",
        ]
        # df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["datetime"] = pd.to_datetime(df["timestamp"], utc=True, unit="s")
        df.drop(columns="timestamp", inplace=True)

        df = df.groupby(pd.Grouper(key="datetime", axis=0, freq="D")).agg(
            new_swap_count=("id", "count"),
            # daily_fee_in_usd=("amountFeeUSD", "sum"),
        )
        idx = pd.date_range(df.index.min(), pd.Timestamp.utcnow())
        df = df.reindex(idx, fill_value=0)
        df.sort_index(ascending=True, inplace=True)
        df["total_swap_count"] = df["new_swap_count"].cumsum()
        # df["total_fee_in_usd"] = df["daily_fee_in_usd"].cumsum()

        return df

    def parse_swaps_data_by_pool(self, data: "JSON") -> pd.DataFrame:
        df = pd.json_normalize(data=data["swaps"])
        df.rename(
            columns={
                "block.block": "block",
                "poolId.id": "pool_id",
            },
            inplace=True,
        )
        # float_columns = [
        #     "amountFeeUSD",
        # ]
        int_columns = [
            "timestamp",
        ]
        # df[float_columns] = df[float_columns].astype(float)
        df[int_columns] = df[int_columns].astype(int)
        df["date"] = pd.to_datetime(df["timestamp"], utc=True, unit="s").dt.date
        df.drop(columns="timestamp", inplace=True)

        df = (
            df.groupby(
                [
                    "pool_id",
                    "date",
                ]
            )
            .agg(
                new_swap_count=("id", "count"),
                # daily_fee_in_usd=("dailyFeeUSD", "sum"),
            )
            .reset_index()
        )

        df["total_swap_count"] = df.groupby(["pool_id"])["new_swap_count"].cumsum()
        # df["total_fee_in_usd"] = df.groupby(["pool_id"])["daily_fee_in_usd"].cumsum()

        return df

    def get_df_pool_day(
        self,
        df_pool_day_data: pd.DataFrame,
        df_swaps_data_by_pool: pd.DataFrame,
    ) -> pd.DataFrame:
        df_pool_day = super().get_df_pool_day(df_pool_day_data, df_swaps_data_by_pool)
        df_pool_day["totalFeeUSD"] = df_pool_day.groupby(["pool_id"])[
            "dailyFeeUSD"
        ].cumsum()

        return df_pool_day
