import pandas as pd
from typing import Dict, List
from pandas.core.frame import DataFrame

# To run --> "python transaction_type.py" 
# Required Package --> pandas 

"""
Problem:
Company A product works on subscription based so the challenge is here is identify and tag if a customer is a new business for the company or had an expansion, contraction, churn and reactivation.
New Business: A new customer subscribed to our product to increase their revenue. Expansion: Customer upgraded their existing plan.
Contraction: Customer downgraded their plan.
Churn: Customer cancelled our subscription
Reactivation: Customer came back
Renewal: Customer renewed our product meaning they finished one cycle and entered a
new cycle
All the subscriptions in the data set are Annual
Task:
Provided the dataset, write a function in python which creates a new column called transaction_type and assign the following tags as described above

Example:
+------------+----------------+--------------+----------------+-------------+----------+
| id         | sub_start_date | sub_end_date | effective_date | total_price | sub_type |
+============+================+==============+================+=============+==========+
| 1          | 5/1/19         | 4/30/20      | 5/1/19         | 190.00      | Active   |
+------------+----------------+--------------+----------------+-------------+----------+
| 1          | 5/1/19         | 4/30/20      | 11/1/19        | 460.00      | Active   |
+------------+----------------+--------------+----------------+-------------+----------+

Expected Answer:
+------------+----------------+--------------+----------------+-------------+----------+------------------+
| id         | sub_start_date | sub_end_date | effective_date | total_price | sub_type | transaction_type |
+============+================+==============+================+=============+==========+==================+
| 1          | 5/1/19         | 4/30/20      | 5/1/19         | 190.00      | Active   | New Business     |
+------------+----------------+--------------+----------------+-------------+----------+------------------+
| 1          | 5/1/19         | 4/30/20      | 11/1/19        | 460.00      | Active   | Expansion        |
+------------+----------------+--------------+----------------+-------------+----------+------------------+
"""

"""
Solution Process:
 - I assumed the ID column represents customer ID
 - "New Business" is the default transaction tag
 - The "filter" method compares the previous and current rows of specific transaction parameters
 # Note: Due to time constraints, this program was not written to scale exponentially
"""


class TransactionType:
	"""Extracts Transaction Types"""

	def get_data(self) -> DataFrame:
		# Read csv file 
		df = pd.read_csv("data.csv")
		df["subscription_start_date"] = pd.to_datetime(df["subscription_start_date"])
		df["subscription_end_date"] = pd.to_datetime(df["subscription_end_date"])
		df["effective_date"] = pd.to_datetime(df["effective_date"])
		df = df.sort_values(by=["ID"])
		df = df.reset_index(drop = True)
		return df
	
	def get_customerID(self) -> Dict:
		# Group data based on customer ID
		grouped_id = self.get_data().groupby(self.get_data().ID)

		customer_ID1 = grouped_id.get_group(1)
		customer_ID2 = grouped_id.get_group(2)
		customer_ID3 = grouped_id.get_group(3)

		customer_ID1 = customer_ID1.values.tolist()
		customer_ID2 = customer_ID2.values.tolist()
		customer_ID3 = customer_ID3.values.tolist()

		return {"customer_ID1" : customer_ID1, "customer_ID2" : customer_ID2, "customer_ID3" : customer_ID3}
	
	def get_rows_customerID1(self) -> Dict:
		# Get rows by customer ID1
		rows = {}
		for i in range(len(self.get_customerID().get("customer_ID1"))):
			rows[f"row{i}"] = self.get_customerID().get("customer_ID1")[i]
		rows["first_row"] = rows.pop("row0")
		rows["second_row"] = rows.pop("row1")
		rows["third_row"] = rows.pop("row2")
		rows["fourth_row"] = rows.pop("row3")
		return rows
	
	def get_rows_customerID2(self) -> Dict:
		# Get rows by customer ID2
		rows = {}
		for i in range(len(self.get_customerID().get("customer_ID2"))):
			rows[f"row{i}"] = self.get_customerID().get("customer_ID2")[i]
		rows["first_row"] = rows.pop("row0")
		rows["second_row"] = rows.pop("row1")
		return rows
	
	def get_rows_customerID3(self) -> Dict:
		# Get rows by customer ID3
		rows = {}
		for i in range(len(self.get_customerID().get("customer_ID3"))):
			rows[f"row{i}"] = self.get_customerID().get("customer_ID3")[i]
		rows["first_row"] = rows.pop("row0")
		rows["second_row"] = rows.pop("row1")
		rows["third_row"] = rows.pop("row2")
		return rows
	
	def handle_rowsCID1(self) -> Dict:
		# Extract total_prices for CID1
		first_row_total_price = self.get_rows_customerID1().get("first_row")[4]
		second_row_total_price = self.get_rows_customerID1().get("second_row")[4]
		third_row_total_price = self.get_rows_customerID1().get("third_row")[4]
		fourth_row_total_price = self.get_rows_customerID1().get("fourth_row")[4]

		# Extract second row columns 
		second_row_subscription_start_date = self.get_rows_customerID1().get("second_row")[1]
		second_row_subscription_end_date = self.get_rows_customerID1().get("second_row")[2]
		second_row_effective_date = self.get_rows_customerID1().get("second_row")[3]

		# Extract third row columns
		third_row_subscription_start_date = self.get_rows_customerID1().get("third_row")[1]
		third_row_subscription_end_date = self.get_rows_customerID1().get("third_row")[2]
		third_row_effective_date = self.get_rows_customerID1().get("third_row")[3]

		# Extract fourth row columns
		fourth_row_subscription_start_date = self.get_rows_customerID1().get("fourth_row")[1]
		fourth_row_subscription_end_date = self.get_rows_customerID1().get("fourth_row")[2]
		fourth_row_effective_date = self.get_rows_customerID1().get("fourth_row")[3]
		
		return {
			"first_row_total_price" : first_row_total_price,
			"second_row_total_price" : second_row_total_price,
			"third_row_total_price" : third_row_total_price,  
			"fourth_row_total_price" : fourth_row_total_price, 

			"second_row_subscription_start_date" : second_row_subscription_start_date,
			"second_row_subscription_end_date" : second_row_subscription_end_date, 
			"second_row_effective_date" : second_row_effective_date, 

			"third_row_subscription_start_date" : third_row_subscription_start_date,
			"third_row_subscription_end_date" : third_row_subscription_end_date, 
			"third_row_effective_date" : third_row_effective_date,

			"fourth_row_subscription_start_date" : fourth_row_subscription_start_date,
			"fourth_row_subscription_end_date" : fourth_row_subscription_end_date, 
			"fourth_row_effective_date" : fourth_row_effective_date
		}

	def handle_rowsCID2(self) -> Dict:
		# Process Customer ID3
		
		# Extract total_prices for CID2
		first_row_total_price = self.get_rows_customerID2().get("first_row")[4]
		second_row_total_price = self.get_rows_customerID2().get("second_row")[4]

		second_row_subscription_start_date = self.get_rows_customerID2().get("second_row")[1]
		second_row_subscription_end_date = self.get_rows_customerID2().get("second_row")[2]
		second_row_effective_date = self.get_rows_customerID2().get("second_row")[3]
		
		return {
			"first_row_total_price" : first_row_total_price,
			"second_row_total_price" : second_row_total_price,

			"second_row_subscription_start_date" : second_row_subscription_start_date,
			"second_row_subscription_end_date" : second_row_subscription_end_date, 
			"second_row_effective_date" : second_row_effective_date, 
		}

	def handle_rowsCID3(self) -> Dict:
		# Process Customer ID3

		# Extract total_prices for CID3
		first_row_total_price = self.get_rows_customerID3().get("first_row")[4]
		second_row_total_price = self.get_rows_customerID3().get("second_row")[4]
		third_row_total_price = self.get_rows_customerID3().get("third_row")[4]

		# Extract second row columns
		second_row_subscription_start_date = self.get_rows_customerID3().get("second_row")[1]
		second_row_subscription_end_date = self.get_rows_customerID3().get("second_row")[2]
		second_row_effective_date = self.get_rows_customerID3().get("second_row")[3]

		# Extract third row columns
		third_row_subscription_start_date = self.get_rows_customerID3().get("third_row")[1]
		third_row_subscription_end_date = self.get_rows_customerID3().get("third_row")[2]
		third_row_effective_date = self.get_rows_customerID3().get("third_row")[3]

		return {
			"first_row_total_price" : first_row_total_price,
			"second_row_total_price" : second_row_total_price,
			"third_row_total_price" : third_row_total_price,

			"second_row_subscription_start_date" : second_row_subscription_start_date,
			"second_row_subscription_end_date" : second_row_subscription_end_date, 
			"second_row_effective_date" : second_row_effective_date, 

			"third_row_subscription_start_date" : third_row_subscription_start_date,
			"third_row_subscription_end_date" : third_row_subscription_end_date, 
			"third_row_effective_date" : third_row_effective_date, 
		}

	def filter(self, effective_date, subscription_start_date, subscription_end_date, current_row_total_price, previous_row_total_price) -> str:
		# Filter rows
		transaction_type = ""
		self.effective_date = effective_date
		self.subscription_start_date  = subscription_start_date
		self.subscription_end_date = subscription_end_date

		self.previous_row_total_price = previous_row_total_price
		self.current_row_total_price = current_row_total_price

		self.effective_start_diff = self.effective_date - self.subscription_start_date
		self.end_start_diff = self.subscription_end_date - self.subscription_start_date 
		
		if self.effective_date > self.subscription_start_date and self.effective_start_diff.days > 30 and self.current_row_total_price > self.previous_row_total_price:
			transaction_type += "Expansion"
		elif self.effective_date > self.subscription_start_date and self.effective_date < self.subscription_end_date and self.current_row_total_price < self.previous_row_total_price:
			transaction_type += "Contraction"
		elif self.effective_date >= self.subscription_start_date and self.effective_date == self.subscription_end_date and self.current_row_total_price == 0.0:
			transaction_type += "Churn"
		elif self.effective_date == self.subscription_start_date and self.end_start_diff.days >= 364 and self.current_row_total_price == self.previous_row_total_price:
			transaction_type += "Renewal"
		elif self.effective_date == self.subscription_start_date and self.previous_row_total_price == 0.0 and self.current_row_total_price > self.previous_row_total_price:
			transaction_type += "Reactivation"
		else:
			transaction_type += "No Subscription"
		return transaction_type
		
	def get_transaction_types(self) -> List:
		return [
			# Filter for Customer ID1 first row - default
			"New Business",
			
			# Second Row vs First Row
			self.filter(
				self.handle_rowsCID1().get("second_row_effective_date"),
				self.handle_rowsCID1().get("second_row_subscription_start_date"),
				self.handle_rowsCID1().get("second_row_subscription_end_date"),
				self.handle_rowsCID1().get("second_row_total_price"),
				self.handle_rowsCID1().get("first_row_total_price")
			),

			# Third row vs Second Row
			self.filter(
				self.handle_rowsCID1().get("third_row_effective_date"),
				self.handle_rowsCID1().get("third_row_subscription_start_date"),
				self.handle_rowsCID1().get("third_row_subscription_end_date"),
				self.handle_rowsCID1().get("third_row_total_price"),
				self.handle_rowsCID1().get("second_row_total_price")
			), 

			# Fourth row vs Third Row 
			self.filter(
				self.handle_rowsCID1().get("fourth_row_effective_date"),
				self.handle_rowsCID1().get("fourth_row_subscription_start_date"),
				self.handle_rowsCID1().get("fourth_row_subscription_end_date"),
				self.handle_rowsCID1().get("fourth_row_total_price"),
				self.handle_rowsCID1().get("third_row_total_price")
			),

			# Filter for Customer ID2 first row - default
			"New Business",

			# Second Row vs First Row
			self.filter(
				self.handle_rowsCID2().get("second_row_effective_date"),
				self.handle_rowsCID2().get("second_row_subscription_start_date"),
				self.handle_rowsCID2().get("second_row_subscription_end_date"),
				self.handle_rowsCID2().get("second_row_total_price"),
				self.handle_rowsCID2().get("first_row_total_price")
			),

			# Filter for Customer ID3 first row - default
			"New Business",

			# Second Row vs First Row
			self.filter(
				self.handle_rowsCID3().get("second_row_effective_date"),
				self.handle_rowsCID3().get("second_row_subscription_start_date"),
				self.handle_rowsCID3().get("second_row_subscription_end_date"),
				self.handle_rowsCID3().get("second_row_total_price"),
				self.handle_rowsCID3().get("first_row_total_price")
			),

			# Third Row vs First Row
			self.filter(
				self.handle_rowsCID3().get("third_row_effective_date"),
				self.handle_rowsCID3().get("third_row_subscription_start_date"),
				self.handle_rowsCID3().get("third_row_subscription_end_date"),
				self.handle_rowsCID3().get("third_row_total_price"),
				self.handle_rowsCID3().get("second_row_total_price")
			), 
		]

	def create_transaction_types_columns(self) -> DataFrame:
		# Merge transaction_type column to the main data frame
		df = self.get_data()
		df["transaction_type"] = pd.Series(self.get_transaction_types()).values
		return df

	
if __name__ == "__main__":
	print(TransactionType().create_transaction_types_columns())