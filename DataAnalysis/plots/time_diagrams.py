import seaborn as sns

from error import WrongArgsError
from plots.utils import (
    save_plot_decorator,
    annotate_plot
)


@save_plot_decorator
def heatmap(df, *columns, rows_limit=40):
    if len(columns) != 3:
        raise WrongArgsError(f"Wrong number of columns: {len(columns)}. Should be 3")
    permitted_types = ["int64", "float64"]
    if df[columns[2]].dtype not in permitted_types:
        raise WrongArgsError(f"Wrong type of last column: {df[columns[2]].dtype}. Valid types: {permitted_types}")

    heatmap_df = df.pivot(index=columns[0], columns=columns[1], values=columns[2])
    heatmap_df[[heatmap_df.isna()]] = 0

    sns.heatmap(heatmap_df[:rows_limit], linewidths=.5)
    annotate_plot(
        title=f"{columns[2].capitalize()} by {columns[1]}",
        x_label=columns[1],
        y_label=columns[0]
    )
