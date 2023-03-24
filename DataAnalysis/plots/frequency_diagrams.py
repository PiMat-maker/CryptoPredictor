import matplotlib.pyplot as plt

from plots.utils import (
    save_plot_decorator,
    annotate_plot
)


@save_plot_decorator
def histogram(df, field, bins):
    plt.hist(df[field], color='green', edgecolor='black', bins=bins)
    annotate_plot(
        title=f"Histogram of {field}",
        x_label=field,
        y_label="amount"
    )
