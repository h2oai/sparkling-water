package water.parser;

public class CategoricalPreviewParseWriter {

  public static byte guessType(String[] domain, int nLines, int nEmpty) {
    final int nStrings = nLines - nEmpty;
    final int nNums = 0;
    final int nDates = 0;
    final int nUUID = 0;
    final int nZeros = 0;

    PreviewParseWriter.IDomain domainWrapper =
        new PreviewParseWriter.IDomain() {
          public int size() {
            return domain.length;
          }

          public boolean contains(String value) {
            for (String domainValue : domain) {
              if (value.equals(domainValue)) return true;
            }
            return false;
          }
        };

    return PreviewParseWriter.guessType(
        nLines, nNums, nStrings, nDates, nUUID, nZeros, nEmpty, domainWrapper);
  }
}
