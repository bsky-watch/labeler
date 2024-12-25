package server

import (
	"errors"

	"gorm.io/gorm"
)

func immediateTransaction(db *gorm.DB, fc func(*gorm.DB) error) error {
	return db.Connection(func(tx *gorm.DB) error {
		tx = tx.Session(&gorm.Session{SkipDefaultTransaction: true})

		err := tx.Exec("BEGIN IMMEDIATE TRANSACTION").Error
		if err != nil {
			return err
		}
		err = fc(tx)
		if err != nil {
			txErr := tx.Exec("ROLLBACK TRANSACTION").Error
			if txErr != nil {
				return errors.Join(err, txErr)
			}
			return err
		}
		return tx.Exec("COMMIT TRANSACTION").Error
	})
}
