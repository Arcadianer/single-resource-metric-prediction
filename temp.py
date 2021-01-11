#jdbcDF

agr=jdbcDF.where(jdbcDF.product.like("OTC_S%")).groupBy("consumption_date").sum("billing_quantity").sort(jdbcDF.consumption_date.asc())
df=agr.select(agr.columns[1]).toPandas()
df.drop(df.tail(1).index,inplace=True)
df.plot()
nacf=acf(df)
ax=pandas.DataFrame({"ACF":nacf}).plot()

#plt.plot(nacf)
print("### ACF ###")
print(nacf)
model=ARIMA(df,order=(14,1,2))
model_fit = model.fit()
# summary of fit model
print(model_fit.summary())
# line plot of residuals

residuals = DataFrame(np.column_stack((df,model_fit.fittedvalues)))

# summary stats of residuals
print(residuals.describe())

# density plot of residuals
residuals.plot()
DataFrame(model_fit.resid).plot()

plt.show()



plt.show()




.toDF(["resource_id","gen_behaviour"])