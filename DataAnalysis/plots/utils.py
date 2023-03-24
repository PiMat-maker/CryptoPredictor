import functools
import matplotlib.pyplot as plt

graph_dir = 'saved_graphs'


def save_plot_decorator(plot_func):

    from datetime import datetime

    @functools.wraps(plot_func)
    def save_plot(*args, **kwargs):
        plot_func(*args, **kwargs)
        plt.savefig(
            f"{graph_dir}/{plot_func.__name__}/{datetime.now().strftime('%Y-%m-%d_%H_%M_%S')}.png",
            format='png',
            dpi=400
        )
        plt.clf()
    return save_plot


def annotate_plot(title, x_label, y_label):
    plt.title(title.capitalize())
    plt.xlabel(x_label.capitalize())
    plt.ylabel(y_label.capitalize())
