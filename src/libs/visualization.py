import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import powerlaw     # DOCS: https://pythonhosted.org/powerlaw/

def similarityMatrice(M, xlabels, ylabels, path):
    fig, ax = plt.subplots(figsize=(16, 16))

    # Plot the matrix
    ax.matshow(M, cmap="Reds")

    ax = plt.gca()

    # Set the plot labels
    ax.set_xticklabels(xlabels)
    ax.set_yticklabels(ylabels)

    # Add text to the plot showing the values at that point
    n = M.shape[0]
    for i in range(n):
        for j in range(n):
            plt.text(j, i, M[i, j], horizontalalignment='center', verticalalignment='center')

    plt.savefig(path)

def drawBoxPlots(data, xlabels, logAxis, savingPath, **kargs):
    # This is lame and ugly, the seaborn ones are much prefered.
    positions = np.arange(len(data)) + 1

    fig = plt.figure(figsize=(10, 10))
    ax = plt.axes()

    # plt.xticks(positions, xlabels)
    bp = ax.boxplot(data, positions=positions, showmeans=True, **kargs)

    if logAxis is True:
        ax.set_yscale('log')

    ax.set_xticklabels(xlabels, rotation=45, ha='right')
    plt.savefig(savingPath)
    # plt.show()
    plt.close()
    return plt


def degreeDistWithPowerLaw(degrees, fileName):

    # The theory behind drawing power-laws: http://networksciencebook.com/chapter/4/#advanced-b

    fig = plt.figure()
    ax = plt.axes()

    values = pd.Series(degrees).sort_values().values
    numBins = np.unique(values).shape[0]
    n, bins, patches = ax.hist(values, numBins)
    plt.yscale('log')

    fit = powerlaw.Fit(values)

    fit.power_law.plot_pdf(ax=ax)   # Plots the probability density function (PDF) of the theoretical distribution for the values given in data
    fit.plot_pdf()  # Plots the probability density function (PDF) (pretty linear)

    '''
    fit.power_law.plot_cdf(survival=True, ax=ax)   # Plots the probability density function (PDF) of the theoretical distribution for the values given in data
    fit.plot_cdf(survival=True)  # Plots the cumulative degree distribution
    '''

    ax.legend([f'PDF of  theoretical distribution', 'PDF (gamma={:.3f})'.format(fit.power_law.alpha)])
    # print('alpha= ', fit.power_law.alpha, '  sigma= ', fit.power_law.sigma)

    plt.savefig(fileName + ".png")
    plt.close()

    return plt