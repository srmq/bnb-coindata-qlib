import asyncio
import argparse
from pathlib import Path
import csv

async def main():
    argParser = argparse.ArgumentParser()
    argParser.add_argument('--source_dir', help='Source directory for unormalized data in qlib OHLCV CSV format')
    argParser.add_argument('--output_dir', help='Output directory for normalized CSV data')

    args = argParser.parse_args()
    if args.source_dir is None:
        raise ValueError('argument for --source_dir is mandatory')
    if args.output_dir is None:
        raise ValueError('argument for --output_dir is mandatory')

    outputDir = Path(args.output_dir)
    if (outputDir.exists()):
        if not outputDir.is_dir():
            raise ValueError('argument for --output_dir exists but is not a directory')
    else:
        outputDir.mkdir(parents=True)

    inputDir = Path(args.source_dir)
    if not inputDir.is_dir():
        raise ValueError('argument for --source_dir is not a directory')

    for csvFilePath in inputDir.glob('*.csv'):
        with open(csvFilePath, newline='') as inputCSVFile:
            csvReader = csv.DictReader(inputCSVFile, delimiter=',')
            outputCSVFilePath = outputDir / csvFilePath.name
            with open(outputCSVFilePath, 'w', newline='') as outputCSVFile:
                outputFields = ['symbol', 'date', 'open', 'close', 'high', 'low', 'volume', 'change', 'factor']
                writer1d = csv.DictWriter(outputCSVFile, fieldnames=outputFields)
                writer1d.writeheader()
                firstRow = True
                factor = None
                lastClose = None
                for row in csvReader:
                    if firstRow:
                        factor = 1.0/float(row['close'])
                        firstRow = False
                    assert factor is not None
                    close = float(row['close'])*factor
                    writer1d.writerow({
                        'symbol': row['symbol'],
                        'date': row['date'],
                        'open' :  float(row['open'])*factor,
                        'close' : close,
                        'high' : float(row['high'])*factor,
                        'low' : float(row['low'])*factor,
                        'volume' : float(row['volume'])/factor,
                        'change' : ('' if lastClose is None else (close/lastClose - 1.0)),
                        'factor' : factor
                        })
                    lastClose = close

if __name__ == "__main__":
    asyncio.run(main())