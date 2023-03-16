
import pandas as pd
import time
from os.path import join
import json 
import numpy as np 
from tqdm import tqdm
import seaborn as sns
from os import listdir, path
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import missingno as msno


import seaborn as sns
import matplotlib.pyplot as plt



def plot_histogram_boxplot(df, columns, color='#607c8e'):
    """
    Esta función crea un histograma y un diagrama de caja para cada columna especificada en la lista 'columns'
    de un DataFrame.

    Parámetros:
    df (pandas DataFrame): DataFrame a analizar.
    columns (list): Lista de columnas para crear el histograma y el diagrama de caja.
    color (str): Color de los histogramas en formato hexadecimal. Por defecto es '#607c8e'.

    Retorna:
    None
    """
    # Obtener el número de filas necesarias para mostrar todos los gráficos
    nrows = len(columns)

    # Crear subplot con nrows filas y 2 columnas
    fig, axs = plt.subplots(nrows=nrows, ncols=2, figsize=(10, 5*nrows))

    for i, column in enumerate(columns):
        # Crear histograma a la izquierda
        axs[i, 0].hist(df[column], bins=20, rwidth=0.9, color=color)
        axs[i, 0].set_title(f'Histograma de {column.capitalize()}')
        axs[i, 0].set_xlabel(column.capitalize())
        axs[i, 0].set_ylabel('Frecuencia')
        axs[i, 0].grid(axis='y', alpha=0.75)

        # Crear diagrama de caja a la derecha
        axs[i, 1].boxplot(df[column], vert=True, widths=0.5, boxprops=dict(linewidth=2), medianprops=dict(linewidth=2), whiskerprops=dict(linewidth=2), capprops=dict(linewidth=2))
        axs[i, 1].set_title(f'Diagrama de caja de {column.capitalize()}')
        axs[i, 1].set_xlabel(column.capitalize())
        axs[i, 1].set_ylabel('Valores')

    # Ajustar el espaciado entre subplots
    plt.tight_layout()

    # Mostrar subplot
    plt.show()




def make_bar_plot(df, target, title, key_map={}, color='#3B3B3B'):
    # Calculate the percentage of values in the target column
    df_pct = (df
              .groupby(target)
              .size()
              .reset_index(name='counts')
              .assign(percentage=lambda x: x['counts'] / x['counts'].sum() * 100)
              .sort_values(by=target, key=lambda x: x.map(key_map))
              )

    # Create a custom color palette
    palette = sns.color_palette([color])

    # Create the bar plot
    sns.barplot(x=df_pct[target], y=df_pct['percentage'], palette=palette)

    # Add a title and x-label to the plot
    plt.title(title, fontsize=16, fontweight='bold')
    plt.xlabel(target, fontsize=14, fontweight='bold')
    plt.ylabel('Porcentaje', fontsize=14, fontweight='bold')

    # Improve y-axis tick labels readability
    yticks = plt.yticks(fontsize=12)

    # Add a horizontal reference line
    reference = df_pct['percentage'].mean()
    plt.axhline(y=reference, color='r', linestyle='dashed', linewidth=1)

    # Add the label for the reference line outside the plot
    plt.annotate(
        f'Promedio: {reference:.2f}',
        xy=(1.01, reference / plt.ylim()[1]),
        xycoords='axes fraction',
        fontsize=10,
        color='r',
        ha='left',
        va='center',
    )

    # Add horizontal grid lines for each tick on the y-axis
    for ytick in yticks[0]:
        plt.axhline(y=ytick, color='white', linestyle='-', linewidth=0.5, alpha=0.7)

    # Show the plot
    plt.show()



def plot_histogram_boxplot_cat(df, columns, category, categories_list, color_palette='viridis'):
    """
    Esta función crea un histograma y un diagrama de caja para cada columna especificada en la lista 'columns'
    de un DataFrame, superponiendo los datos de cada categoría en la misma gráfica.

    Parámetros:
    df (pandas DataFrame): DataFrame a analizar.
    columns (list): Lista de columnas para crear el histograma y el diagrama de caja.
    category (str): Columna categórica utilizada para separar los datos.
    categories_list (list): Lista de categorías en el orden deseado para las leyendas.
    color_palette (str): Nombre de la paleta de colores a usar. Por defecto es 'viridis'.

    Retorna:
    None
    """
    num_categories = len(categories_list)
    colors = plt.get_cmap(color_palette)(np.linspace(0, 1, num_categories))

    nrows = len(columns)
    fig, axs = plt.subplots(nrows=nrows, ncols=2, figsize=(10, 5 * nrows))

    for i, column in enumerate(columns):
        for j, (cat, color) in enumerate(zip(categories_list, colors)):
            cat_df = df[df[category] == cat]

            axs[i, 0].hist(cat_df[column], bins=20, rwidth=0.9, color=color, alpha=0.7, label=f'{category}: {cat}')
            axs[i, 0].set_title(f'Histograma de {column.capitalize()}')
            axs[i, 0].set_xlabel(column.capitalize())
            axs[i, 0].set_ylabel('Frecuencia')
            axs[i, 0].grid(axis='y', alpha=0.75)

            # Dibujar boxplots individuales con una posición específica y un color específico
            axs[i, 1].boxplot(cat_df[column], positions=[j], widths=0.5, patch_artist=True, boxprops=dict(facecolor=color, linewidth=2), medianprops=dict(linewidth=2), whiskerprops=dict(linewidth=2), capprops=dict(linewidth=2))

        axs[i, 1].set_title(f'Diagrama de caja de {column.capitalize()}')
        axs[i, 1].set_xticks(range(num_categories))
        axs[i, 1].set_xticklabels(categories_list)
        axs[i, 1].set_xlabel(category)
        axs[i, 1].set_ylabel('Valores')

    # Añadir leyendas a los histogramas
    for ax in axs[:, 0]:
        ax.legend()

    plt.tight_layout()
    plt.show()



def make_bar_plot_cat(df, target, category, categories_list, title, color_palette='viridis',annotate_text='Promedio: {:.2f}', target_list=None):
    """
    Esta función crea un gráfico de barras mostrando el porcentaje de valores en una columna objetivo de un DataFrame,
    agrupados por una columna categórica.

    Parámetros:
    df (pandas.DataFrame): El DataFrame a graficar.
    target (str): El nombre de la columna a utilizar como objetivo del gráfico.
    category (str): El nombre de la columna categórica para agrupar los datos.
    categories_list (list): Lista de categorías en el orden deseado para las leyendas.
    title (str): El título del gráfico.
    color_palette (str): Nombre de la paleta de colores a usar. Por defecto es 'viridis'.
    annotate_text (str): Texto para la línea de referencia horizontal que muestra el promedio.
    target_list (list): Lista de categorías de destino en el orden deseado para las barras.

    Retorna:
    None
    """
    if target_list is None:
        target_list = df[target].unique()

    num_categories = len(categories_list)
    colors = plt.get_cmap(color_palette)(np.linspace(0, 1, num_categories))

    df_pct = (df
              .groupby([category, target])
              .size()
              .unstack(fill_value=0)
              .apply(lambda x: x / x.sum() * 100, axis=1)
              .reindex(index=categories_list, columns=target_list)
              .reset_index()
              .melt(id_vars=category, var_name=target, value_name='percentage')
              )

    sns.barplot(x=target, y='percentage', hue=category, data=df_pct, palette=colors)

    plt.title(title, fontsize=16, fontweight='bold')
    plt.xlabel(target, fontsize=14, fontweight='bold')
    plt.ylabel('Porcentaje', fontsize=14, fontweight='bold')

    yticks = plt.yticks(fontsize=12)

    reference = df_pct['percentage'].mean()
    plt.axhline(y=reference, color='r', linestyle='dashed', linewidth=1)

    plt.annotate(
        annotate_text,
        xy=(1.01, reference / plt.ylim()[1]),
        xycoords='axes fraction',
        fontsize=10,
        color='r',
        ha='left',
        va='center',
    )

    for ytick in yticks[0]:
        plt.axhline(y=ytick, color='white', linestyle='-', linewidth=0.5, alpha=0.7)

    plt.show()
