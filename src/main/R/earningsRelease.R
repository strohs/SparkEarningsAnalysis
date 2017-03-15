require(ggplot2)
require(gridExtra)
#read csv as dataframe
df= read.csv("E:\\wmtMoves.csv",colClasses = c("integer","Date","Date","Date","numeric","Date","Date","numeric"))
df$relDate = as.factor( format(df$relDate,"%Y") )
#combine relDate year and qtr into a factor of YEAR-QTR
#df$qtr = as.factor( paste( format(df$relDate,"%Y"),df$qtr,sep="-"))


be <- ggplot(df, aes(x=qtr)) +
  geom_col( aes( y=bMove, fill=relDate ), position = "dodge" ) +
  labs(title="Price Move Before Earnings Release", x="quarter", y="max move")

ae <- ggplot(df, aes(x=qtr)) +
  geom_col( aes( y=aMove, fill=relDate ), position = "dodge" ) +
  labs(title="Price Move After Earnings Release", x="quarter", y="max move")

grid.arrange(be, ae, ncol=2)