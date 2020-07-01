package util

func WaitSuccess(f func() error, errHook func(err error), doneHook func()) {
	for {
		if err := f(); err != nil {
			if errHook != nil {
				errHook(err)
			}
			continue
		}
		if doneHook != nil {
			doneHook()
		}
		break
	}
}

func Sequential(fns ...func() error) error {
	for _, fn := range fns {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
