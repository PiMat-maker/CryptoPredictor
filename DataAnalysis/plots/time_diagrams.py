from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

import error


def heatmap(df, *columns, rows_limit=40):
    print(columns)
    if len(columns) != 3:
        raise error.WrongArgsError(f"Wrong number of columns: {len(columns)}. Should be 3")
    permitted_types = ["int64", "float64"]
    if df[columns[2]].dtype not in permitted_types:
        raise error.WrongArgsError(f"Wrong type of last column: {df[columns[2]].dtype}. Valid types: {permitted_types}")

    heatmap_df = df.pivot(index=columns[0], columns=columns[1], values=columns[2])
    heatmap_df[[heatmap_df.isna()]] = 0

    svm = sns.heatmap(heatmap_df[:rows_limit], linewidths=.5)
    figure = svm.get_figure()
    figure.savefig(
        f"{columns[2]}_heatmap_by_{columns[0]}_to_{columns[1]}_{datetime.now().strftime('%Y-%m-%d_%H_%M_%S')}.png",
        format='png',
        dpi=400
    )
    plt.show()
