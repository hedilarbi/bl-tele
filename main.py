import qrcode

# URL for the QR code
url = "https://g.co/kgs/47bWUP8"

# Generate QR Code
qr = qrcode.QRCode(
    version=10,
    error_correction=qrcode.constants.ERROR_CORRECT_H,  # High error correction
    box_size=10,
    border=4,
)
qr.add_data(url)
qr.make(fit=True)

# Create and save the QR code image
qr_img = qr.make_image(fill_color="black", back_color="white")
qr_img.save("qrcode.png")
qr_img.show()
