import sqlite3

class ShipmentProcessor:
    def __init__(self, db_path):
        self.db_path = db_path

    def process_shipment(self, item_name, quantity, log_callback):
        """
        Executes the shipment logic.
        :param item_name: Name of the item
        :param quantity: Amount to move
        :param log_callback: A function to print to the GUI console
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        log_callback(f"--- STARTING TRANSACTION: Move {quantity} of {item_name} ---")

        try:
            # STEP 1: Update Inventory safely (prevent CHECK constraint)
            cursor.execute(
                """
                UPDATE inventory
                SET stock_qty = stock_qty - ?
                WHERE item_name = ? AND stock_qty >= ?
                """,
                (quantity, item_name, quantity),
            )

            # If no row was updated, stock was insufficient or item missing
            if cursor.rowcount != 1:
                raise RuntimeError("Not enough stock available or item does not exist")

            log_callback(">> STEP 1 SUCCESS: Inventory Deducted.")

            # STEP 2: Log the Shipment
            cursor.execute(
                "INSERT INTO shipment_log (item_name, qty_moved) VALUES (?, ?)",
                (item_name, quantity),
            )
            log_callback(">> STEP 2 SUCCESS: Shipment Logged.")

            # Commit ONLY if both steps succeeded
            conn.commit()
            log_callback("--- TRANSACTION COMMITTED ---")

        except Exception as e:
            conn.rollback()
            log_callback(f"--- TRANSACTION ROLLED BACK: {e} ---")

        finally:
            conn.close()
