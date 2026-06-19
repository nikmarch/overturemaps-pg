SELECT * FROM (VALUES
    (1, 'restaurant', 'US', 'e16ad9d1-6f3f-4914-b0b1-be9da720e9f3'),
    (2, 'hotel',      'DE', 'e16ad9d1-6f3f-4914-b0b1-be9da720e9f3'),
    (3, 'gas_station','JP', 'e16ad9d1-6f3f-4914-b0b1-be9da720e9f3')
) AS t(id, category, country, sample_id)
