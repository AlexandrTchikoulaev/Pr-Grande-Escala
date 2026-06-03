from docx import Document

doc = Document("relatorio final.docx")
paras = doc.paragraphs

# Print paras 383 to 410 (section 3.2.2)
print("=== Section 3.2.2 (paras 383-415) ===")
for i in range(383, 416):
    text = paras[i].text.strip()
    if text:
        print(f"Para {i} | style={paras[i].style.name!r}: {repr(text)}")
    else:
        print(f"Para {i} | style={paras[i].style.name!r}: (empty)")
