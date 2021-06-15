import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import powerlaw     # DOCS: https://pythonhosted.org/powerlaw/
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
from matplotlib.lines import Line2D

def matrixHeatMap(Ms, xlabels, ylabels, path, titles):

    fig, ax = plt.subplots(nrows=1, ncols=len(Ms), figsize=(35, 15))

    for it, M in enumerate(Ms):
        # Plot
        ax[it].matshow(M, cmap="Reds")

        # ax[it] = plt.gca()

        ax[it].set_aspect(0.8)
        ax[it].set_title(titles[it], y=-0.15, x=0.5)

        # Set the plot labels
        ax[it].set_yticks(list(range(len(ylabels))))
        ax[it].set_yticklabels(ylabels, rotation=20)
        ax[it].set_xticks(list(range(len(xlabels))))
        ax[it].set_xticklabels(xlabels, rotation=90)

        # Add text to the plot showing the values at that point
        n = M.shape[0]
        for i in range(n):
            for j in range(n):
                ax[it].text(j, i, M[i, j], horizontalalignment='center', verticalalignment='center')

    plt.tight_layout()
    # plt.show()
    plt.savefig(path)
    plt.close()


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


def smallMultipleWordCloud(data, WCmaxWords=10, WCwidth=800, WCheight=400, groupMemberships=[], colorLabels={}):
    """
    :param data: List of word corpus
    :param WCmaxWords:
    :param WCwidth:
    :param WCheight:
    :return:
    """
    class SimpleGroupedColorFunc(object):

        def __init__(self, members, colors):
            self.members = members
            self.colors = colors
            print(members.keys())
            print(len(members))

        def __call__(self, word, **kwargs):

            for entityType, data in self.members.items():
                if word in data:
                    return self.colors[entityType]

            print('UNKOWWWWWN group - ERRROR CRASHING')
            exit()

            """ 
            if word in self.members['tag']:
                return self.colors['tag']
                # return "hsl(10, 80, 60)"
            elif word in self.members['location']:
                return self.colors['location']
                # return "hsl(240, 80, 60)"
            else:
                
                print('UNKOWWWWWN group - ERRROR CRASHING')
                exit()
            """

    fig = plt.figure()
    print(len(data))
    for i, (txt, clrs) in enumerate(zip(data, groupMemberships)):
        ax = fig.add_subplot(4, 3, i+1)
        # wordcloud = createWordCloud(txt, WCmaxWords, WCwidth, WCheight)  # mask= for special form
        wordcloud = WordCloud(background_color="white", max_words=WCmaxWords, width=WCwidth, height=WCheight)  # mask= for special form
        wordcloud.generate_from_frequencies(txt)

        if len(groupMemberships)>0:
            print('COLORING')
            print('COLORING')
            color_func = SimpleGroupedColorFunc(clrs, colorLabels)
            wordcloud = wordcloud.recolor(color_func=color_func)

        ax.imshow(wordcloud, interpolation='bilinear') # bilinear so the displayed image appears more smoothly - https://matplotlib.org/gallery/images_contours_and_fields/interpolation_methods.html
        ax.axis('off')

        print(i)
        if i == len(data)-2:
            handles, labels = ax.get_legend_handles_labels()

            handles = [Line2D([0], [0], color=c, lw=4, label=l) for l, c in colorLabels.items()]
            # labels = list(groupMemberships['colors'].keys())
            fig.legend(handles=handles, loc='lower right',  prop={'size': 6})


    print('SAVING SAVING SAVING')
    plt.savefig('wordclouds.png', dpi=300)
    print('SAVING SAVING SAVING')

def createWordCloud(text, WCmaxWords,WCwidth, WCheight, **kwargs):
    # Create and generate a word cloud image:
    wordcloud = WordCloud(width=WCwidth, height=WCheight, max_words=WCmaxWords, random_state=42) \
        .generate(text, **kwargs)
    return wordcloud
