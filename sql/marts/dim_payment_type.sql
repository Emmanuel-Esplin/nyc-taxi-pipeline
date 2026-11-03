-- dim_payment_type: codes from the TLC Yellow Taxi data dictionary (March 2025 revision).
SELECT * FROM (
    VALUES
        (0, 'Flex Fare trip'),
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
) AS t(payment_type, payment_description)
